""" A few example writers that we'll use to illustrate the basics

We'll write to batch, streaming sinks and also a postgres sink

"""
from datetime import datetime
from typing import List

from pyspark.sql import *


def write_sink(df: DataFrame, output_path: str, sink: List[str], checkpoint_path: str = None):
    """A very simplistic factory - just for example purposes

    Args:
        :param checkpoint_path: for spark streaming - where to keep tracking info
        :param sink: a list of output types - the list is not really being used
        :param output_path: where to put output files
        :param df: the dataframe to write


    Returns:
        * void - the files are stored

    """
    streaming: bool = checkpoint_path is not None

    if streaming:
        write_stream_to_filestream(df, output_path, checkpoint_path)
    elif 'jdbc' in sink:
        write_batch_to_jdbc(df)
    else:
        write_batch_to_file(df, output_path)


def write_batch_to_file(mail_df: DataFrame, mail_folder: str):
    """ write a batch file"""
    x = datetime.now()
    sub_folder = f"year={x.year}/month={x.month:02}/day={x.day:02}/hour={x.hour:02}"

    mail_df.write.json(mail_folder + sub_folder)
    print(f"processed {mail_df.count()} records")


def write_batch_to_jdbc(df):
    """write a batch to jdbc"""
    wr = df.write.format("jdbc").mode("append")\
        .options("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "ses_mail") \
        .option("user", "postgres") \
        .option("password", "chinois1")\
        .save()


def write_stream_to_filestream(mail_df: DataFrame, mail_folder: str, checkpoint_path: str):
    """write a stream to a file"""
    x = datetime.now()
    sub_folder = f"year={x.year}/month={x.month:02}/day={x.day:02}/hour={x.hour:02}"

    mail_df \
        .writeStream \
        .option("path", mail_folder + sub_folder) \
        .option("checkpointLocation", f"{checkpoint_path}/mail") \
        .format('json') \
        .start() \
        .awaitTermination()



