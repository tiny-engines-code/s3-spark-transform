from datetime import datetime
from datetime import datetime
from time import sleep
from typing import Any, List

import pandas as pd
import psycopg2
from psycopg2._psycopg import connection

# CONTOLLER_TOPIC="test_worker"
CONTOLLER_TABLE = "public.controller"
ARCHIVE_TABLE = "public.archive_files"
CONTOLLER_READY_STATUS = "ready"
CONTOLLER_WORKING_STATUS = "working"

FILE_TABLE = "public.raw_files"
FILE_READY_STATUS = "ready"
FILE_WORKING_STATUS = "working"
FILE_DONE_STATUS = "done"
# PAGE_SIZE=25
FETCH_SIZE=500


class Controller:

    conn: connection
    controller_topic : str

    def __init__(self, conn, topic):
        self.controller_topic = topic
        self.conn = conn

    # ---- create table -----
    def controller_up(self):
        try:
            cur = self.conn.cursor()
            sql_create = f"""create table if not exists {CONTOLLER_TABLE} (
            topic text not null constraint controller_pk primary key, 
            status text,
            insert_time timestamp(6) default CURRENT_TIMESTAMP not null,        
            modified_time timestamp(6))"""

            cur.execute(sql_create)
            cur.close()

        except Exception as e:
            print(e)
            raise e

    # ---- create table -----
    def controller_down(self):
        try:
            cur = self.conn.cursor()
            sql_create = f"""drop table if exists {CONTOLLER_TABLE} """

            cur.execute(sql_create)
            cur.close()

        except Exception as e:
            print(e)
            raise e

    def get_controller_status(self) -> (str, int, int):
        cur = self.conn.cursor()
        sql = f"""select status, modified_time from {CONTOLLER_TABLE} where topic = '{self.controller_topic}' """
        results = pd.read_sql_query(sql, self.conn)
        if results.size == 0:
            return (None, None, 0)
        status = results.iloc[0]['status']
        mod = results.iloc[0]["modified_time"]
        age_seconds = (pd.Timedelta(datetime.now() - mod.to_pydatetime())).seconds

        cur.close()
        return (status, age_seconds, 1)

    def upsert_new_job(self, status: str):
        cur = self.conn.cursor()
        sql = f"""insert into {CONTOLLER_TABLE} (topic, status, modified_time)  
        values ('{self.controller_topic}', '{status}', current_timestamp)
        on conflict(topic) do update set (status, modified_time)   = ('{status}', current_timestamp) """
        cur.execute(sql)
        cur.close()

    def set_job_status(self, status: str):
        cur = self.conn.cursor()
        sql = f"""update {CONTOLLER_TABLE} set status = '{status}', modified_time = current_timestamp where topic = '{self.controller_topic}' """
        cur.execute(sql)
        cur.close()

    def intitialize_job(self) -> bool:
        # --- controller up --
        self.controller_up()

        # -- is this job running ---
        status, age_seconds, recs = self.get_controller_status()
        if recs == 0:
            self.upsert_new_job(status=CONTOLLER_WORKING_STATUS)
        elif status != CONTOLLER_READY_STATUS:
            print("BUSY: "+status)
            return False
        else:
            self.set_job_status(status=CONTOLLER_WORKING_STATUS)
        return True

    def complete_job(self):
        # -- set to done s ---
        self.set_job_status(status=CONTOLLER_READY_STATUS)


