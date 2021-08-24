from datetime import datetime
from typing import List

from pyspark.sql import *


def write_batch_to_file(mail_df: DataFrame, mail_folder: str):
    x = datetime.now()
    sub_folder = f"year={x.year}/month={x.month:02}/day={x.day:02}/hour={x.hour:02}"

    mail_df.write.json(mail_folder + sub_folder)
    print(f"processed {mail_df.count()} records")


def write_batch_to_jdbc(df):
    db_target_url = "jdbc:postgresql://localhost:5432/postgres"  # db_target_url = "jdbc:postgresql://cds-kafka-sink.cfdzoccamj4b.us-east-2.rds.amazonaws.com:5432/postgres"

    v = df.write.format("jdbc").mode("append")\
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "ses_mail") \
        .option("user", "postgres") \
        .option("password", "chinois1")\
        .save()


def write_stream_to_filestream(mail_df: DataFrame, mail_folder: str, checkpoint_path: str):
    x = datetime.now()
    sub_folder = f"year={x.year}/month={x.month:02}/day={x.day:02}/hour={x.hour:02}"

    mail_query = mail_df \
        .writeStream \
        .option("path", mail_folder + sub_folder) \
        .option("checkpointLocation", f"{checkpoint_path}/mail") \
        .format('json') \
        .start() \
        .awaitTermination()


def jdbc_connect_options(df, epoch_id):
    df.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "public.ses_mail") \
        .option("user", "postgres") \
        .option("password", "chinois1").save()


def write_sink(df: DataFrame, destination: str, sink: List[str], checkpoint_path: str = None):
    streaming: bool = checkpoint_path is not None

    df.printSchema()

    if streaming:
        if 'jdbc' in sink:
            df.writeStream.foreachBatch(jdbc_connect_options).start()
            df.awaitTermination()
        else:
            write_stream_to_filestream(df, destination, checkpoint_path)
    else:
        if 'jdbc' in sink:
            write_batch_to_jdbc(df)

        else:
            write_batch_to_file(df, destination)
