import sys
import json
import boto3
from datetime import datetime, timezone, date
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

PROC_PATH = f's3://{BUCKET}/{BASE}/processed/transactions/'
CURA_PATH = f's3://{BUCKET}/{BASE}/curated/daily_summary/'

# Read TODAY'S processed partition only
today           = date.today()
today_str       = today.strftime('%Y-%m-%d')
today_partition = f'{PROC_PATH}transaction_date={today_str}/'

print(f"📅 Today's date      : {today_str}")
print(f"📂 Reading partition : {today_partition}")

# Check partition exists before reading
response = s3.list_objects_v2(Bucket=BUCKET, Prefix=f'{BASE}/processed/transactions/transaction_date={today_str}/')

if response.get('KeyCount', 0) == 0:
    print(f"⚠️  No processed data found for {today_str} — run raw-to-processed job first.")
    job.commit()
    sys.exit(0)

df = spark.read.option("basePath", PROC_PATH).parquet(today_partition)

record_count = df.count()
print(f"✅ Records loaded : {record_count}")

if record_count == 0:
    print("⚠️  Partition is empty — exiting.")
    job.commit()
    sys.exit(0)

df.printSchema()

# Aggregation 1 — Daily summary
print("\n📊 Building daily summary...")

df_daily = (df
    .groupBy(
        'transaction_date',
        'channel',
        'merchant_category',
        'transaction_status',
        'amount_bucket'
    )
    .agg(
        F.count('*')                              .alias('total_transactions'),
        F.round(F.sum('amount'), 2)               .alias('total_amount'),
        F.round(F.avg('amount'), 2)               .alias('avg_amount'),
        F.round(F.max('amount'), 2)               .alias('max_amount'),
        F.round(F.min('amount'), 2)               .alias('min_amount'),
        F.sum(F.col('is_high_value').cast('int')) .alias('high_value_count'),
        F.sum(F.col('is_failed').cast('int'))     .alias('failed_count'),
        F.sum(F.col('is_debit').cast('int'))      .alias('debit_count'),
        F.countDistinct('account_id')             .alias('unique_accounts'),
        F.countDistinct('merchant_name')          .alias('unique_merchants')
    )
)

daily_count = df_daily.count()
print(f"   Daily summary rows : {daily_count}")

# overwrites today's partition only
(df_daily
    .write
    .mode('overwrite')
    .partitionBy('transaction_date')
    .option('compression', 'snappy')
    .parquet(f'{CURA_PATH}channel_category_summary/')
)
print("✅ Daily summary written")


# Aggregation 2 — Account level summary

print("\n📊 Building account summary...")

df_account = (df
    .groupBy('transaction_date', 'account_id')
    .agg(
        F.count('*')                                   .alias('total_transactions'),
        F.round(F.sum('amount'), 2)                    .alias('total_amount'),
        F.sum(F.col('is_failed').cast('int'))          .alias('failed_count'),
        F.sum(F.col('is_high_value').cast('int'))      .alias('high_value_count'),
        F.sum(F.col('is_debit').cast('int'))           .alias('debit_count'),
        F.sum(F.col('is_failed').cast('int'))          .alias('total_failed_amount'),
        F.round(F.min('balance_after_transaction'), 2) .alias('min_balance'),
        F.round(F.max('balance_after_transaction'), 2) .alias('max_balance'),
        F.round(F.last('balance_after_transaction'), 2).alias('latest_balance')
    )
)

account_count = df_account.count()
print(f"   Account summary rows : {account_count}")

(df_account
    .write
    .mode('overwrite')
    .partitionBy('transaction_date')
    .option('compression', 'snappy')
    .parquet(f'{CURA_PATH}account_summary/')
)
print("✅ Account summary written")

print("\n" + "="*50)
print("✅ PROCESSED → CURATED COMPLETE")
print(f"   Date processed       : {today_str}")
print(f"   Records aggregated   : {record_count}")
print(f"   Daily summary rows   : {daily_count}")
print(f"   Account summary rows : {account_count}")


job.commit()
