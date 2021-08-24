import os

from pyspark.sql import *
from pyspark.sql.types import *
from transformer import transform_ses_records
from writers import write_sink

# --------------------------------------------------
#  ses record schema
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


# --------------------------------------------------
#  read as a batch
#
def read_json_file(spark: SparkSession, event_type: str, input_folder: str):
    raw = spark.read.schema(input_schema).json(input_folder)
    return transform_ses_records(raw)


# --------------------------------------------------
#  read as a stream
#
def read_json_streaming(spark: SparkSession, event_type: str, input_folder: str):
    raw = spark.readStream.schema(input_schema).json(input_folder)
    return transform_ses_records(raw)



