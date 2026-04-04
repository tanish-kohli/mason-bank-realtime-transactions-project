import random
import uuid
import json
import time
import boto3
from faker import Faker
from datetime import datetime
from kafka import KafkaProducer
from botocore.exceptions import ClientError


# AWS Secrets Manager Helper
def get_secret(secret_name: str, region_name: str = "ap-south-1") -> dict:
    """Fetch a secret from AWS Secrets Manager and return it as a dict."""
    client = boto3.client("secretsmanager", region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise RuntimeError(f"Failed to retrieve secret '{secret_name}': {e}") from e

    secret_string = response.get("SecretString")
    if not secret_string:
        raise ValueError(f"Secret '{secret_name}' has no SecretString value.")
    return json.loads(secret_string)


# Config from Secrets Manager
secrets = get_secret("kafka/config")
BOOTSTRAP_SERVERS = secrets["bootstrap_servers"]


# Kafka Configuration
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    request_timeout_ms=30000,
)

fake = Faker()
channels = ["UPI", "ATM", "CARD", "NEFT", "RTGS", "IMPS"]
transaction_status = ["SUCCESS", "FAILED"]
merchant_categories = [
    "GROCERY", "RESTAURANT", "FUEL", "TRAVEL",
    "ECOMMERCE", "ENTERTAINMENT", "UTILITIES",
]
banks = ["HDFC", "ICICI", "SBI", "AXIS", "KOTAK"]

# Simulated account balances
accounts = {}

def generate_account():
    account_id = "AC" + str(random.randint(100000, 109999))
    accounts[account_id] = random.randint(5000, 100000)
    return account_id

def generate_transaction(account_id):
    amount = round(random.uniform(50, 10000), 2)
    txn_type = random.choice(["DEBIT", "CREDIT"])
    status = random.choice(transaction_status)
    balance = accounts[account_id]

    if status == "SUCCESS":
        balance = balance - amount if txn_type == "DEBIT" else balance + amount
        accounts[account_id] = balance

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "transaction_type": txn_type,
        "amount": amount,
        "currency": "INR",
        "balance_after_transaction": round(accounts[account_id], 2),
        "transaction_timestamp": datetime.utcnow().isoformat(),
        "channel": random.choice(channels),
        "merchant_name": fake.company(),
        "merchant_category": random.choice(merchant_categories),
        "counterparty_account": "AC" + str(random.randint(100000, 999999)),
        "counterparty_bank": random.choice(banks),
        "reference_number": str(uuid.uuid4())[:12],
        "transaction_status": status,
        "branch_id": "BR" + str(random.randint(100, 999)),
    }

def stream_transactions():
    account_list = [generate_account() for _ in range(10)]
    while True:
        account = random.choice(account_list)
        txn = generate_transaction(account)
        producer.send(
            "transactions_topic",
            key=txn["account_id"].encode("utf-8"),
            value=txn,
        )
        print("Sent:", txn["transaction_id"])
        time.sleep(random.uniform(1, 5))

if __name__ == "__main__":
    stream_transactions()