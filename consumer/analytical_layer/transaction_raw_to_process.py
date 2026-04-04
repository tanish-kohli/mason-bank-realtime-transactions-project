import sys
import json
import boto3
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET'])
BUCKET = args['S3_BUCKET']
BASE   = 'mason_bank_real_time_streaming_project'

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3 = boto3.client('s3')

RAW_PATH      = f's3://{BUCKET}/{BASE}/raw/transactions/'
PROC_PATH     = f's3://{BUCKET}/{BASE}/processed/transactions/'
WATERMARK_KEY = f'{BASE}/watermark/raw_to_processed_watermark.json'

# Watermark helpers 
def read_watermark():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=WATERMARK_KEY)
        wm  = json.loads(obj['Body'].read().decode('utf-8'))
        ts  = datetime.fromisoformat(wm['last_processed_timestamp'])
        print(f"📌 Watermark: {ts}")
        return ts
    except:
        print("📌 No watermark — first run, processing all files")
        return datetime(1970, 1, 1, tzinfo=timezone.utc)

def write_watermark(ts):
    s3.put_object(
        Bucket      = BUCKET,
        Key         = WATERMARK_KEY,
        Body        = json.dumps({
            'last_processed_timestamp': ts.isoformat(),
            'last_run_utc':             ts.strftime('%Y-%m-%d %H:%M:%S UTC')
        }, indent=2),
        ContentType = 'application/json'
    )
    print(f"💾 Watermark updated → {ts.isoformat()}")

# Get new raw files since watermark 
def get_new_raw_files(last_processed):
    new_files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f'{BASE}/raw/transactions/'):
        for obj in page.get('Contents', []):
            key           = obj['Key']
            last_modified = obj['LastModified']
            if last_processed.tzinfo is None:
                last_processed = last_processed.replace(tzinfo=timezone.utc)
            if last_modified > last_processed and key.endswith('.json'):
                new_files.append(f's3://{BUCKET}/{key}')
    return new_files

# Main
run_start      = datetime.now(timezone.utc)
last_processed = read_watermark()

print(f"\n🔍 Scanning for new files since {last_processed}...")
new_files = get_new_raw_files(last_processed)

if not new_files:
    print("✅ No new files — nothing to process. Exiting.")
    job.commit()
    sys.exit(0)

print(f"   Found {len(new_files)} new file(s)")

df_raw    = spark.read.option("multiline", "false").json(new_files)
raw_count = df_raw.count()
print(f"   Raw records : {raw_count}")

if raw_count == 0:
    print("✅ Files empty — exiting.")
    job.commit()
    sys.exit(0)

# Transformations 
print("\n🔧 Transforming...")

df_processed = (df_raw
    .withColumn('amount',
        F.col('amount').cast(DoubleType()))
    .withColumn('balance_after_transaction',
        F.col('balance_after_transaction').cast(DoubleType()))
    .withColumn('transaction_timestamp',
        F.to_timestamp('transaction_timestamp'))
    .withColumn('_ingestion_timestamp',
        F.to_timestamp('_ingestion_timestamp'))
    .withColumn('_kafka_offset',
        F.col('_kafka_offset').cast(LongType()))
    .withColumn('_kafka_partition',
        F.col('_kafka_partition').cast(IntegerType()))
    .withColumn('transaction_date',
        F.to_date('transaction_timestamp'))
    .withColumn('transaction_hour',
        F.hour('transaction_timestamp'))
    .withColumn('transaction_day_of_week',
        F.dayofweek('transaction_timestamp'))
    .withColumn('is_high_value',
        F.when(F.col('amount') > 8000, True).otherwise(False))
    .withColumn('is_failed',
        F.when(F.col('transaction_status') == 'FAILED', True).otherwise(False))
    .withColumn('is_debit',
        F.when(F.col('transaction_type') == 'DEBIT', True).otherwise(False))
    .withColumn('amount_bucket',
        F.when(F.col('amount') < 500,  'LOW')
         .when(F.col('amount') < 3000, 'MEDIUM')
         .when(F.col('amount') < 8000, 'HIGH')
         .otherwise('VERY_HIGH'))
    .dropna(subset=['transaction_id', 'account_id', 'amount',
                    'transaction_status', 'transaction_timestamp'])
    .dropDuplicates(['transaction_id'])
)

proc_count = df_processed.count()
print(f"✅ After transforms : {proc_count} records")
print(f"   Dropped         : {raw_count - proc_count} (nulls/dupes)")

# Write processed Parquet 
print(f"\n💾 Writing to processed zone...")
(df_processed
    .write
    .mode('append')
    .partitionBy('transaction_date')
    .option('compression', 'snappy')
    .parquet(PROC_PATH)
)
print("✅ Processed zone written")

write_watermark(run_start)

print("\n" + "="*50)
print("✅ RAW → PROCESSED COMPLETE")
print(f"   Files processed  : {len(new_files)}")
print(f"   Records written  : {proc_count}")
print("="*50)

job.commit()