import os
from time import sleep

import psycopg2
from psycopg2._psycopg import connection

from controller import Controller,  CONTOLLER_READY_STATUS, FILE_DONE_STATUS
from dequeue_files import DeQueueFiles

os.environ['RDS_HOST'] = "cds-kafka-sink.cfdzoccamj4b.us-east-2.rds.amazonaws.com"
os.environ['RDS_USERNAME'] = "nike"
os.environ['RDS_DB_NAME'] = "postgres"
os.environ['SECRET_NAME'] = "justdoit"


def get_connection(host, db, user, password) -> connection:
    conn = psycopg2.connect(host=host, dbname=db, user=user, password=password)
    conn.autocommit = True
    return conn


if __name__ == "__main__":
    rds_host = os.environ['RDS_HOST']
    user = os.environ['RDS_USERNAME']
    db_name = os.environ['RDS_DB_NAME']
    password = os.environ['SECRET_NAME']

    topic = "notification_status"

    conn = get_connection(host=rds_host, db=db_name, user=user, password=password)

    # -- controller --
    controller = Controller(conn=conn, topic=topic)
    dequeue = DeQueueFiles(conn=conn)

    if not controller.intitialize_job():
        print("Job skipped")
        exit(0)

    try:
        file_count = dequeue.checkout_files(topic=topic)
        if file_count <= 0:
            print("no files to process")
            exit(0)

        # 0--
        # do work
        dequeue.set_files_status(FILE_DONE_STATUS)
    except Exception as e:
        pass
    finally:
        controller.set_job_status(status=CONTOLLER_READY_STATUS)
