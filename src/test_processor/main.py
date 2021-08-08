import os
from typing import Dict

import psycopg2
from psycopg2._psycopg import connection

from src.controller import Controller
from src.controller import DeQueueFiles
from src.lambda_processor import lambda_function
from postgre_sandbox.string_config import FILE_TABLE, CONTOLLER_TABLE, CONTOLLER_TOPIC, FILE_DONE_STATUS, \
    CONTOLLER_READY_STATUS

# os.environ['RDS_HOST'] = "localhost"
# os.environ['RDS_USERNAME'] = "postgres"
# os.environ['RDS_DB_NAME'] = "postgres"
# os.environ['SECRET_NAME'] = "chinois1"

os.environ['RDS_HOST'] = "cds-kafka-sink.cfdzoccamj4b.us-east-2.rds.amazonaws.com"
os.environ['RDS_USERNAME'] = "nike"
os.environ['RDS_DB_NAME'] = "postgres"
os.environ['SECRET_NAME'] = "justdoit"


def mock_events(start: int, end: int) -> Dict:
    records = []
    while start <= end:

        records.append(
                {
                    "eventVersion": "2.0",
                    "eventSource": "aws:s3",
                    "awsRegion": "us-east-1",
                    "eventTime": "2021-01-01T00:00:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {
                        "principalId": "EXAMPLE"
                    },
                    "requestParameters": {
                        "sourceIPAddress": "127.0.0.1"
                    },
                    "responseElements": {
                        "x-amz-request-id": "EXAMPLE123456789",
                        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "testConfigRule",
                        "bucket": {
                            "name": "example-bucket",
                            "ownerIdentity": {
                                "principalId": "EXAMPLE"
                            },
                            "arn": "arn:aws:s3:::example-bucket"
                        },
                        "object": {
                            "key": f"data/notification_status/2021/03/04/05/cds_file_{str(start).zfill(3)}/key",
                            "size": 1024,
                            "eTag": "0123456789abcdef0123456789abcdef",
                            "sequencer": "0A1B2C3D4E5F678901"
                        }
                    }
                }
            )
        start += 1

    return dict(Records=records)


# ---- create table -----
def controller_down(conn):
    try:
        cur = conn.cursor()
        sql_create = f"""drop table if exists {CONTOLLER_TABLE} """

        cur.execute(sql_create)
        cur.close()

    except Exception as e:
        print(e)
        raise e


def raw_files_down(conn):
    try:
        cur = conn.cursor()
        sql_create = f"""drop table if exists {FILE_TABLE} """
        cur.execute(sql_create)
        cur.close()

    except Exception as e:
        print(e)
        raise e


def get_connection(host, db, user, password) -> connection:
    conn = psycopg2.connect(host=host, dbname=db, user=user, password=password)
    conn.autocommit = True
    return conn


if __name__ == "__main__":
    events = mock_events(100,120)
    lambda_function.lambda_handler(event=events, context=None)

    rds_host = os.environ['RDS_HOST']
    user = os.environ['RDS_USERNAME']
    db_name = os.environ['RDS_DB_NAME']
    password = os.environ['SECRET_NAME']

    conn = get_connection(host=rds_host, db=db_name, user=user, password=password)

    # -- controller --
    controller = Controller(conn=conn, topic=CONTOLLER_TOPIC)
    dequeue = DeQueueFiles(conn=conn)

    if not controller.intitialize_job():
        print("Job skipped")
        exit(0)

    file_count = dequeue.checkout_files()
    if file_count <= 0:
        print("no files to process")
        exit(0)

    # 0--
    # do work
    controller.set_job_status(worker=CONTOLLER_TOPIC, status=CONTOLLER_READY_STATUS)
    dequeue.set_files_status(FILE_DONE_STATUS)
