import sys
import json
import boto3
from kafka import KafkaConsumer
from datetime import datetime
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, [
    'KAFKA_BROKER', 'TOPIC_NAME', 'S3_BUCKET', 'AWS_REGION'
])

BROKER    = args['KAFKA_BROKER']
TOPIC     = args['TOPIC_NAME']
BUCKET    = args['S3_BUCKET']
REGION    = args['AWS_REGION']
BASE_PATH = 'mason_bank_real_time_streaming_project'

s3 = boto3.client('s3', region_name=REGION)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    group_id='analytics-service',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    consumer_timeout_ms=60000,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("📊 Analytics consumer started — writing to S3 raw zone...")

batch        = []
batch_size   = 50      # write every 50 events
events_total = 0

for message in consumer:
    txn = message.value
    events_total += 1

    # Add metadata fields
    txn['_ingestion_timestamp'] = datetime.utcnow().isoformat()
    txn['_kafka_offset']        = message.offset
    txn['_kafka_partition']     = message.partition

    batch.append(json.dumps(txn))

    # Flush batch to S3 raw zone
    if len(batch) >= batch_size:
        now       = datetime.utcnow()
        partition = (f"year={now.year}/month={now.month:02d}/"
                     f"day={now.day:02d}/hour={now.hour:02d}")
        key       = (f"{BASE_PATH}/raw/transactions/{partition}/"
                     f"batch_{now.strftime('%H%M%S')}_{events_total}.json")

        s3.put_object(
            Bucket      = BUCKET,
            Key         = key,
            Body        = '\n'.join(batch),   # newline-delimited JSON
            ContentType = 'application/json'
        )
        print(f"✅ Wrote {len(batch)} events → s3://{BUCKET}/{key}")
        batch = []

# Flush remaining events
if batch:
    now       = datetime.utcnow()
    partition = (f"year={now.year}/month={now.month:02d}/"
                 f"day={now.day:02d}/hour={now.hour:02d}")
    key       = (f"{BASE_PATH}/raw/transactions/{partition}/"
                 f"final_{now.strftime('%H%M%S')}.json")
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body='\n'.join(batch), ContentType='application/json')
    print(f"✅ Final flush — {len(batch)} events → {key}")

print(f"\n✅ Done | Total events written: {events_total}")
consumer.close()