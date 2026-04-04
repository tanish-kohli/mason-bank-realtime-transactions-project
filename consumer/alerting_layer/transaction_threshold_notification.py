import sys
import json
import boto3
import csv
import io
from kafka import KafkaConsumer
from awsglue.utils import getResolvedOptions

# Job Arguments
args = getResolvedOptions(sys.argv, [
    'KAFKA_BROKER',
    'TOPIC_NAME',
    'S3_BUCKET',
    'S3_MASTER_KEY',
    'SES_SENDER',
    'AWS_REGION'
])

BROKER        = args['KAFKA_BROKER']       # 13.233.164.130:9092
TOPIC         = args['TOPIC_NAME']         # transactions_topic
S3_BUCKET     = args['S3_BUCKET']          # tanish-data-engineering-project-bucket
S3_MASTER_KEY = args['S3_MASTER_KEY']      # mason_bank_real_time_streaming_project/raw/mason_bank_user_master.csv
SES_SENDER    = args['SES_SENDER']         # tanishkohlitech@gmail.com
REGION        = args['AWS_REGION']         # ap-south-1 (Mumbai, since your EC2 is 13.233.x = ap-south-1)

# Alert Thresholds 
HIGH_VALUE_THRESHOLD = 9000.00   # INR — tune as needed
LOW_BALANCE_THRESHOLD = 100.00  # INR — alert if balance drops below this

# Load User Master from S3 
def load_user_master(bucket, key):
    """
    Loads mason_bank_user_master.csv into a dict keyed by account_id.
    Returns: { 'AC100000': {'user_name': 'Tanish Kohli', 'user_email': '...'}, ... }
    """
    s3       = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    content  = response['Body'].read().decode('utf-8')
    reader   = csv.DictReader(io.StringIO(content))
    return {
        row['account_id']: {
            'user_name':  row['user_name'],
            'user_email': row['user_email']
        }
        for row in reader
    }

print("📂 Loading Mason Bank user master from S3...")
user_master = load_user_master(S3_BUCKET, S3_MASTER_KEY)
print(f"✅ Loaded {len(user_master)} accounts: {list(user_master.keys())}")

# Email Templates 
def build_email(alert_type, user_name, txn):
    amount   = txn['amount']
    balance  = txn['balance_after_transaction']
    status   = txn['transaction_status']
    txn_type = txn['transaction_type']
    channel  = txn['channel']
    merchant = txn['merchant_name']
    category = txn['merchant_category']
    acc_id   = txn['account_id']
    txn_id   = txn['transaction_id']
    ref_no   = txn['reference_number']
    ts       = txn['transaction_timestamp']

    if alert_type == 'HIGH_VALUE':
        subject = f"🚨 Mason Bank Alert: Large Transaction of ₹{amount:,.2f} on your account"
        body = f"""
                Dear {user_name},
                
                A high-value transaction has been detected on your Mason Bank account.
                
                ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                  TRANSACTION DETAILS
                ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                  Account ID          : {acc_id}
                  Transaction ID      : {txn_id}
                  Reference Number    : {ref_no}
                  Type                : {txn_type}
                  Amount              : ₹{amount:,.2f}
                  Channel             : {channel}
                  Merchant            : {merchant}
                  Category            : {category}
                  Status              : {status}
                  Balance After Txn   : ₹{balance:,.2f}
                  Timestamp           : {ts} UTC
                ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                
                If you did not authorize this transaction, please contact Mason Bank 
                support immediately at support@masonbank.com or call 1800-XXX-XXXX.
                
                Regards,
                Mason Bank Fraud & Alerts Team
                        """.strip()
                
    elif alert_type == 'FAILED_TXN':
                subject = f"⚠️ Mason Bank Alert: Transaction of ₹{amount:,.2f} FAILED"
                body = f"""
        Dear {user_name},
        
        Your recent transaction has FAILED. Here are the details:
        
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
          FAILED TRANSACTION DETAILS
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
          Account ID          : {acc_id}
          Transaction ID      : {txn_id}
          Reference Number    : {ref_no}
          Type                : {txn_type}
          Amount              : ₹{amount:,.2f}
          Channel             : {channel}
          Merchant            : {merchant}
          Category            : {category}
          Status              : ❌ FAILED
          Balance After Txn   : ₹{balance:,.2f}
          Timestamp           : {ts} UTC
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        Your account balance remains unchanged for this transaction.
        If you continue to face issues, please retry or contact support.
        
        Regards,
        Mason Bank Transaction Support Team
                """.strip()

    elif alert_type == 'LOW_BALANCE':
        subject = f"⚠️ Mason Bank Alert: Low Balance Warning — ₹{balance:,.2f} remaining"
        body = f"""
                Dear {user_name},
                
                Your Mason Bank account balance has dropped below ₹{LOW_BALANCE_THRESHOLD:,.2f}.
                
                ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                  BALANCE ALERT
                ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                  Account ID          : {acc_id}
                  Current Balance     : ₹{balance:,.2f}
                  Last Transaction    : ₹{amount:,.2f} ({txn_type} via {channel})
                  Merchant            : {merchant}
                  Timestamp           : {ts} UTC
                ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                
                Please add funds to avoid transaction failures.
                
                Regards,
                Mason Bank Account Services Team
                        """.strip()
                
    return subject, body
                
# SES Sender
ses = boto3.client('ses', region_name=REGION)

def send_alert_email(user_info, alert_type, txn):
    user_name  = user_info['user_name']
    user_email = user_info['user_email']

    subject, body = build_email(alert_type, user_name, txn)

    try:
        ses.send_email(
            Source=SES_SENDER,
            Destination={'ToAddresses': [user_email]},
            Message={
                'Subject': {'Data': subject},
                'Body':    {'Text': {'Data': body}}
            }
        )
        print(f"📧 [{alert_type}] Email sent → {user_name} ({user_email}) | "
              f"Amount: ₹{txn['amount']:,.2f} | Status: {txn['transaction_status']}")
        return True

    except Exception as e:
        print(f"❌ Failed to send email to {user_email}: {str(e)}")
        return False

# Kafka Consumer 
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    group_id='notifications-service',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    consumer_timeout_ms=60000,        
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"🔔 Mason Bank Notifications Consumer started | Topic: {TOPIC}")
print(f"   Thresholds → High Value: ₹{HIGH_VALUE_THRESHOLD:,} | Low Balance: ₹{LOW_BALANCE_THRESHOLD:,}\n")

alerts_sent  = 0
events_total = 0

for message in consumer:
    txn        = message.value
    account_id = txn.get('account_id')
    amount     = float(txn.get('amount', 0))
    balance    = float(txn.get('balance_after_transaction', 0))
    status     = txn.get('transaction_status', '').upper()
    txn_type   = txn.get('transaction_type', '')

    events_total += 1

    # Lookup account in master data
    user_info = user_master.get(account_id)

    if not user_info:
        # Account not in master data (producer generates random AC IDs beyond your 8 users)
        print(f"[SKIP] account_id={account_id} not in master data | "
              f"amount=₹{amount:,.2f} | status={status}")
        continue

    # Alert Rule 1: High value transaction (any status)
    if amount > HIGH_VALUE_THRESHOLD:
        if send_alert_email(user_info, 'HIGH_VALUE', txn):
            alerts_sent += 1

    # Alert Rule 2: Failed transaction
    if status == 'FAILED':
        if send_alert_email(user_info, 'FAILED_TXN', txn):
            alerts_sent += 1

    #  Alert Rule 3: Low balance after a DEBIT
    if txn_type == 'DEBIT' and status == 'SUCCESS' and balance < LOW_BALANCE_THRESHOLD:
        if send_alert_email(user_info, 'LOW_BALANCE', txn):
            alerts_sent += 1

    
    if (amount <= HIGH_VALUE_THRESHOLD
            and status != 'FAILED'
            and not (txn_type == 'DEBIT' and balance < LOW_BALANCE_THRESHOLD)):
        print(f"[OK] {account_id} | ₹{amount:,.2f} | {status} | "
              f"balance=₹{balance:,.2f}")

print(f"\n✅ Session complete | Events processed: {events_total} | Alerts sent: {alerts_sent}")
consumer.close()