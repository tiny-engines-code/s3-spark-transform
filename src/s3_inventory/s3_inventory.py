import datetime
import os
import typing
from typing import Dict
from typing import List

import boto3
import psycopg2
import psycopg2.extras
from psycopg2._psycopg import connection

# bucket Name
bucket_name = 'cds-data-temp'
staging_schema = "public"
# bucket Resource
s3 = boto3.resource('s3')
FILE_TABLE = "public.process_queue"


class Inventory:
    conn: connection
    files_array: List[Dict] = []
    topic: str
    stage_table: str
    folder_name: str
    bucket: typing.Any
    lookback_delta: datetime.timedelta

    def __init__(self):
        rds_host = os.environ['RDS_HOST']
        user = os.environ['RDS_USERNAME']
        db_name = os.environ['RDS_DB_NAME']
        password = os.environ['SECRET_NAME']

        self.stage_table = f"{staging_schema}.processed_sweeper"
        self.bucket = s3.Bucket(bucket_name)

        self.set_connection(host=rds_host, db=db_name, user=user, password=password)

    def sweep(self, prefix: str, topic: str, lookback_days):
        self.lookback_delta = datetime.timedelta(days=lookback_days)
        self.stage_table = f"{staging_schema}.processed_sweeper"
        self.folder_name = f"{prefix}/{topic}"
        self.topic = topic
        self.get_inventory()
        self.stage_up()
        self.insert_batch(status="ready")

    # ---- get a postgres connection -----
    def set_connection(self, host, db, user, password):
        conn = psycopg2.connect(host=host, dbname=db, user=user, password=password)
        conn.autocommit = True
        self.conn = conn

    # ---- create table -----
    def stage_up(self):
        try:
            cur = self.conn.cursor()
            sql_create = f"""create table if not exists {self.stage_table} (
            bucket text,
            filename text primary key,
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

    def stage_down(self):
        try:
            cur = self.conn.cursor()
            sql_create = f"""drop table if exists {self.stage_table} """
            cur.execute(sql_create)
            cur.close()

        except Exception as e:
            print(e)
            raise e

    # ---- create table -----
    def process_queue_up(self):
        try:
            cur = self.conn.cursor()
            sql_create = f"""create table if not exists {FILE_TABLE} (
            bucket text,
            filename text primary key,
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

    def get_orphans(self):
        try:
            with self.conn.cursor() as cursor:
                sql = """select N.bucket, N.filename
                         from processed_sweeper N
                                  left join processed_streaming S on N.filename = S.filename
                     where S.filename is NULL"""
                cursor.execute(sql)
                return cursor.fetchall()

        except Exception as e:
            print(e)
            raise e

    def merge_orphans(self):
        try:
            self.process_queue_up()
            with self.conn.cursor() as cursor:
                sql = """insert
                    into public.process_queue (bucket, filename, topic, status, created_time, insert_time, modified_time)
                        (select N.bucket, N.filename, N.topic, N.status, N.created_time, N.insert_time, N.modified_time
                         from processed_sweeper N
                                  left join processed_streaming S on N.filename = S.filename
                     where S.filename is NULL) ON CONFLICT DO NOTHING"""
                cursor.execute(sql)

                sql = """delete from processed_sweeper o
                            USING (
                                select  o2.filename from processed_sweeper o2
                                left join process_queue t on t.filename = o2.filename
                                where t.filename is not NULL ) A"""

                cursor.execute(sql)

        except Exception as e:
            print(e)
            raise e

    # --- insert one batch -----
    def insert_batch(self, status: str) -> None:
        now = datetime.datetime.now()
        try:
            with self.conn.cursor() as cursor:
                all_files = list(
                    map(lambda x: (x['bucket'], x['key'], x['topic'], status, x['event_time'], now), self.files_array))
                psycopg2.extras.execute_batch(cursor,
                                              f"""INSERT INTO {self.stage_table} (bucket, filename, topic, status, created_time, modified_time) 
                                              VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                                              all_files)
        except Exception as e:
            print(f"SQL fail {e}  ")

    def get_inventory(self):
        print(f"Getting all files from {self.folder_name}")
        for file in self.bucket.objects.filter(Prefix=self.folder_name):
            current_time = datetime.datetime.now(datetime.timezone.utc)
            back_date = current_time - self.lookback_delta
            file_date = file.last_modified.replace(tzinfo=datetime.timezone.utc)
            if file_date <= back_date:
                self.files_array.append(
                    dict(bucket=bucket_name, key=file.key, topic=self.topic, event_time=file.last_modified))
            else:
                print(f"Skipping bucket={bucket_name}, key={file.key}")
