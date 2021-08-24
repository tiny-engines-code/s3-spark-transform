import os

from pyspark.sql import *
from pyspark.sql.types import *
from transformer import transform_df
from writers import write_sink

input_schema = StructType([
    StructField('eventType', StringType(), True),
    StructField('delivery', StringType(), True),
    StructField('send', StringType(), True),
    StructField('reject', StringType(), True),
    StructField('open', StringType(), True),
    StructField('bounce', StringType(), True),
    StructField('click', StringType(), True),
    StructField('complaint', StringType(), True),
    StructField('mail', StructType([
        StructField('timestamp', StringType(), True),
        StructField('source', StringType(), True),
        StructField('sendingAccountId', StringType(), True),
        StructField('messageId', StringType(), True),
        StructField('destination', ArrayType(StringType()), True),
        StructField('headersTruncated', BooleanType(), True),
        StructField('tags', MapType(StringType(), ArrayType(StringType())), True),
        StructField('headers', ArrayType(
            StructType([
                StructField('name', StringType(), True),
                StructField('value', StringType(), True)])), True),

        StructField('commonHeaders', StructType([
            StructField('from', ArrayType(StringType()), True),
            StructField('to', ArrayType(StringType()), True),
            StructField('messageId', StringType(), True),
            StructField('subject', StringType(), True)]), True)

    ]), True),  # mail
])


def read_json_file(spark: SparkSession, event_type: str, input_folder: str):
    raw = spark.read.schema(input_schema).json(input_folder)
    return transform_df(raw)


def read_json_streaming(spark: SparkSession, event_type: str, input_folder: str):
    raw = spark.readStream.schema(input_schema).json(input_folder)
    return transform_df(raw)


def process_s3_directory(spark: SparkSession, event_type: str, source_path: str, sink_path: str,
                         checkpoint_path: str = None):
    source_path = os.path.join(source_path, event_type + "/")
    sink_path = os.path.join(sink_path, event_type + "/")

    if checkpoint_path is not None:
        mail_df = read_json_streaming(spark=spark, event_type=event_type, input_folder=source_path)
        write_sink(mail_df, sink_path, ["stream"], checkpoint_path)
    else:
        mail_df = read_json_file(spark=spark, event_type=event_type, input_folder=source_path)
        if sink_path.startswith("jdbc"):
            write_sink(mail_df, sink_path, ["jdbc"])
        elif checkpoint_path is None:
            write_sink(mail_df, sink_path, ["file"])
        else:
            write_sink(mail_df, sink_path, ["stream"])
