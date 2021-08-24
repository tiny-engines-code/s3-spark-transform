from datetime import datetime
from typing import List

from pyspark.sql import *


def write_batch_to_file(mail_df: DataFrame, mail_folder: str):
    x = datetime.now()
    sub_folder = f"year={x.year}/month={x.month:02}/day={x.day:02}/hour={x.hour:02}"

    mail_df.write.json(mail_folder + sub_folder)
    print(f"processed {mail_df.count()} records")


def write_batch_to_jdbc(df):

    wr = df.write.format("jdbc").mode("append")\
        .options("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "ses_mail") \
        .option("user", "postgres") \
        .option("password", "chinois1")\
        .save()


def write_stream_to_filestream(mail_df: DataFrame, mail_folder: str, checkpoint_path: str):
    x = datetime.now()
    sub_folder = f"year={x.year}/month={x.month:02}/day={x.day:02}/hour={x.hour:02}"

    mail_df \
        .writeStream \
        .option("path", mail_folder + sub_folder) \
        .option("checkpointLocation", f"{checkpoint_path}/mail") \
        .format('json') \
        .start() \
        .awaitTermination()


def write_stream_to_jdbc(df, epoch_id=None):
    df.writeStream.format("jdbc").mode("append")\
        .options("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "ses_mail") \
        .option("user", "postgres") \
        .option("password", "chinois1")\
        .start()\
        .awaitTermination()


def write_sink(df: DataFrame, destination: str, sink: List[str], checkpoint_path: str = None):
    streaming: bool = checkpoint_path is not None

    df.printSchema()

    if streaming:
        if 'jdbc' in sink:
            write_stream_to_jdbc(df)
        else:
            write_stream_to_filestream(df, destination, checkpoint_path)
    else:
        if 'jdbc' in sink:
            write_batch_to_jdbc(df)

        else:
            write_batch_to_file(df, destination)
