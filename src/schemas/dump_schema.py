import json
from collections import OrderedDict
from datetime import datetime
from time import sleep
from typing import Any, List

import pandas as pd
import psycopg2
from psycopg2._psycopg import connection

notification_table = "sandbox.fields_notification_status"


def dump_service_status(conn: connection):
    sql = f"select * from {notification_table} where service_status and version > 0 order by field_ordinal"
    return dump_schema(conn, sql, "ServiceStatus")


def dump_notification_status(conn: connection):
    sql = f"select * from {notification_table} where version > 0 order by field_ordinal"
    return dump_schema(conn, sql, "NotificationStatus")


def dump_schema(conn: Any, sql: str, schema_name: str) -> str:
    cur = conn.cursor()
    results = pd.read_sql_query(sql, conn)
    db_schema = results.to_dict(orient='records')
    fields = []

    for field_def in db_schema:

        record = OrderedDict()
        record["name"] = field_def['field_name']

        if field_def['field_type'] in ['string', 'int', 'long']:
            if field_def['nullable']:
                record["type"] = ['null', field_def['field_type']]
            else:
                record["type"] = field_def['field_type']

        if field_def['field_type'] == "string":
            record["avro.java.string"] = 'String'

        if field_def['field_type'] == "timestamp":
            record["type"] = "long"
            record["logicalType"] = "timestamp-millis"

        record["doc"] = field_def['field_doc']

        fields.append(record)

    schema = dict(type="record", namespace="com.nike.notificationpublisher.model", name="NotificationStatus",
                  fields=fields)
    return json.dumps(schema)

if __name__ == "__main__":
    conn = psycopg2.connect(host="localhost", dbname="postgres", user="postgres", password="chinois1")
    print(dump_notification_status(conn=conn))
    print(dump_service_status(conn=conn))
