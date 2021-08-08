import os
from typing import Dict

import psycopg2
from psycopg2._psycopg import connection

# os.environ['RDS_HOST'] = "localhost"
# os.environ['RDS_USERNAME'] = "postgres"
# os.environ['RDS_DB_NAME'] = "postgres"
# os.environ['SECRET_NAME'] = "chinois1"
from src.s3_inventory.s3_inventory import Inventory

os.environ['RDS_HOST'] = "cds-kafka-sink.cfdzoccamj4b.us-east-2.rds.amazonaws.com"
os.environ['RDS_USERNAME'] = "nike"
os.environ['RDS_DB_NAME'] = "postgres"
os.environ['SECRET_NAME'] = "justdoit"


if __name__ == "__main__":
    inventory = Inventory()
    inventory.sweep(prefix="ses/mail", topic="bounce", lookback_days=0)
    inventory.sweep(prefix="ses/mail", topic="open", lookback_days=0)
    inventory.sweep(prefix="ses/mail", topic="send", lookback_days=0)
    inventory.sweep(prefix="ses/mail", topic="delivery", lookback_days=0)
    inventory.sweep(prefix="ses/mail", topic="reject", lookback_days=0)

    for row in inventory.get_orphans():
        print(f"DO: s3a//{row[0]}/{row[1]}")

    print("bye")
