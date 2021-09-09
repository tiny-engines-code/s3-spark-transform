""" read all SES json files in a folder and it's subdirectories

Example of different functions for both batch and streaming that read all files for a specific event class,
call a common transformer, and return the transformed dataframe

"""
import os

from pyspark.sql import *
from pyspark.sql.types import *
from transformer import transform_ses_records
from writers import write_sink

# --------------------------------------------------
#  uber ses event record schema with ALL event class
#  elements - we'll parse out what we need
#
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


def read_json_file(pc: SparkSession, event_class: str, input_path: str) -> DataFrame:
    """Performs a batch read, and returns a transformed Dataframe

    Args:
        :param pc: the spark session
        :param event_class: one of "open", "delivery", "complaint", "send", "reject", "bounce", "click"
        :param input_path: root directory for the input files

    Returns:
        * A transformed dataframe

    """

    raw = pc.read.schema(input_schema).json(input_path)
    return transform_ses_records(raw, event_class)


def read_json_streaming(pc: SparkSession, event_class: str, input_path: str) -> DataFrame:
    """Performs a structured streaming read, and returns a transformed Dataframe

    Args:
        :param pc: the spark session
        :param event_class: one of "open", "delivery", "complaint", "send", "reject", "bounce", "click"
        :param input_path: root directory for the input files

    Returns:
        * A transformed dataframe

    """
    raw = pc.readStream.schema(input_schema).json(input_path)
    return transform_ses_records(raw, event_class)



