from datetime import datetime
from datetime import datetime
from time import sleep
from typing import Any, List

import pandas as pd
import psycopg2
from psycopg2._psycopg import connection


# ---- get a postgres connection -----
from controller import ARCHIVE_TABLE
from controller import CONTOLLER_TABLE, FILE_TABLE, FILE_READY_STATUS, FETCH_SIZE, \
    CONTOLLER_WORKING_STATUS, CONTOLLER_READY_STATUS, FILE_WORKING_STATUS, FILE_DONE_STATUS


class DeQueueFiles:

    conn: connection
    files_array : List

    def __init__(self, conn):
        if conn != None:
            self.conn = conn
        else:
            self.conn = self.get_connection(host="localhost", db="postgres", user="postgres", password="chinois1")
        files_array = []

    # ---- create table -----
    def archive_files_up(self):
        try:
            cur = self.conn.cursor()
            sql_create = f"""create table if not exists {ARCHIVE_TABLE} (
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

    def get_connection(self, host, db, user, password) -> connection:
        conn = psycopg2.connect(host=host, dbname=db, user=user, password=password)
        conn.autocommit = True
        return conn

    def get_open_files(self, topic) -> List:
        cur = self.conn.cursor()
        sql = f"select bucket, file_name from {FILE_TABLE} where status= '{FILE_READY_STATUS}' and topic = '{topic}' order by insert_time  limit {FETCH_SIZE} "
        results = pd.read_sql_query(sql, self.conn)
        files = results.to_dict(orient='records')
        return files

    def archive_files_batches(self):
        cur = self.conn.cursor()
        file_count = len(self.files_array)
        page_size = 100
        page_start = 0
        page_end = page_start + page_size

        while page_start < file_count:
            slice = ",".join(map(lambda x: f"""'{x['file_name']}'""",self.files_array[page_start:page_end]))
            sql = f"""insert into {ARCHIVE_TABLE} select *
             from {FILE_TABLE} 
             where status = '{FILE_DONE_STATUS}' """
            cur.execute(sql)

            sql = f"""delete 
             from {FILE_TABLE} 
             where status = '{FILE_DONE_STATUS}' """
            cur.execute(sql)

            page_start = page_end
            page_end = page_start + min(page_size,file_count)

        cur.close()

    def set_files_status(self, status: str):
        cur = self.conn.cursor()
        file_count = len(self.files_array)
        page_size = 100
        page_start = 0
        page_end = page_start + page_size

        while page_start < file_count:
            slice = ",".join(map(lambda x: f"""'{x['file_name']}'""",self.files_array[page_start:page_end]))
            sql = f"""update {FILE_TABLE} set status = '{status}', modified_time=current_timestamp 
            where file_name in ({slice}) """

            cur.execute(sql)
            page_start = page_end
            page_end = page_start + min(page_size,file_count)

        cur.close()

    def checkout_files(self, topic) -> int:
        self.files_array: List = self.get_open_files(topic=topic)
        self.set_files_status(status=FILE_WORKING_STATUS)
        return len(self.files_array)

    def archive_files(self) -> int:
        self.archive_files_up()
        self.archive_files_batches()

