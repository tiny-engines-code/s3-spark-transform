import json
import os
import re
import urllib
from datetime import datetime
from typing import List, Dict, Any
import dateutil.parser
import boto3
import psycopg2
import psycopg2.extras
import typing
from psycopg2._psycopg import connection
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

CONTOLLER_TOPIC="controller"
CONTOLLER_TABLE = "public.controller"
CONTOLLER_READY_STATUS = "ready"
CONTOLLER_WORKING_STATUS = "working"

FILE_TABLE = "public.raw_files"
FILE_READY_STATUS = "ready"
FILE_WORKING_STATUS = "working"
FILE_DONE_STATUS = "done"
# PAGE_SIZE=25
FETCH_SIZE=500

VALID_TOPICS = ["notification_status"]

s3 = boto3.client('s3')


class QueueFiles:
    conn : connection

    def __init__(self):
        rds_host = os.environ['RDS_HOST']
        user = os.environ['RDS_USERNAME']
        db_name = os.environ['RDS_DB_NAME']
        password = os.environ['SECRET_NAME']
        self.set_connection(host=rds_host, db=db_name, user=user, password=password)

    # ---- get a postgres connection -----
    def set_connection(self, host, db, user, password):
        conn = psycopg2.connect(host=host, dbname=db, user=user, password=password)
        conn.autocommit = True
        self.conn =  conn

    # ---- create table -----
    def raw_files_up(self):
        try:
            cur = self.conn.cursor()
            sql_create = f"""create table if not exists {FILE_TABLE} (
            bucket text,
            file_name text primary key,
            topic text,
            status text, 
            created_time timestamp(6),
            insert_time timestamp(6) default CURRENT_TIMESTAMP not null,
            modified_time timestamp(6))"""

            cur.execute(sql_create)
            self.conn.commit()
            cur.close()

        except Exception as e:
            print(e)
            raise e

    # --- insert one batch -----
    def insert_batch(self, files: List[str], status: str) -> None:
        now = datetime.now()
        try:
            with self.conn.cursor() as cursor:
                all_files = list(map(lambda x: (x['bucket'], x['key'], x['topic'], status, x['event_time'], now), files))
                psycopg2.extras.execute_batch(cursor,
                                              f"""INSERT INTO {FILE_TABLE} (bucket, file_name, topic, status, created_time, modified_time) 
                                              VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                                              all_files)
        except Exception as e:
            logger.error(f"SQL fail {e}  ")

    #
    def try_string_date(self, event_string: str) -> datetime:
        try:
            event_time = dateutil.parser.isoparse(event_string)
            return event_time
        except Exception as e:
            print(e)
            return datetime(1970,1,1)

    def try_get_topic(self, key: str) -> str:
        try:
            m = re.search(r""".*/(.*)/(year=)?\d\d\d\d/""", key)
            topic = m.group(1)
        except Exception as e:
            logger.error('Failed parse topic from : '+ str(e))
            topic = "Unknown"
        return topic

    # --- main worker ----
    def list_files(self, event : Dict, context: Any) -> List[str]:
        logger.info("raw_files_up")
        self.raw_files_up()

        # Get the object from the event and show its content type
        files = []
        for record in event['Records']:
            logger.info("process one record")
            event_time = self.try_string_date(record['eventTime'])
            bucket = record['s3']['bucket']['name']
            logger.info(f"bucket={bucket}")

            key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
            logger.info(f"key={key}")

            topic = self.try_get_topic(key=key)
            logger.info(f"topic={topic}")

            if topic.lower() not in VALID_TOPICS:
                logger.info("FILTERED OUT")
                continue
            try:
                files.append(dict(bucket=bucket, key=key, topic=topic, event_time=event_time))
            except Exception as e:
                print(e)
                logger.error(
                    'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(
                        key, bucket))
                raise e

        logger.info(f"list_files RETURNING {len(files)} files " )
        return files

    def close(self):
        if self.conn is not None:
            self.conn.close()


def lambda_handler(event, context):
    rds_host = os.environ['RDS_HOST']
    user = os.environ['RDS_USERNAME']
    db_name = os.environ['RDS_DB_NAME']
    password = os.environ['SECRET_NAME']

    queue = QueueFiles()
    item_count = 0
    try:
        # queue.set_connection(host=rds_host, db=db_name, user=user, password=password)
        files = queue.list_files(event, context)
        item_count = len(files)
        queue.insert_batch(files, FILE_READY_STATUS)
    except Exception as e:
        pass
    finally:
        queue.close()

    content = "Inserted %d items to RDS table" % (item_count)
    response = {
        "statusCode": 200,
        "body": content,
        "headers": {
            'Content-Type': 'text/html',
        }
    }
    return response



