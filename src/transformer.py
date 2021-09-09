""" transform various SES schemas into a common schema

Create a flat schema that can be databased and pivoted across all event classes

"""
import json
import re

from pyspark.sql import *
from pyspark.sql.functions import col, lit, lower, input_file_name, udf, current_timestamp, to_timestamp, count, when
from pyspark.sql.types import *

__all__ = ["DataFrameReader", "DataFrameWriter"]

# --------------------------------------------------
#  temp schemas
#
header_schema = StructType([
    StructField("send_id", StringType(), False)])

event_schema = StructType([
    StructField("event_time", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_subclass", StringType(), False),
    StructField("event_recipients", StringType(), False)
])

file_schema = StructType([
    StructField("bucket", StringType(), False),
    StructField("filename", StringType(), False)
])


def transform_ses_records(raw: DataFrame, event_class: str) -> DataFrame:
    """Transforms a raw SES Dataframe into a common schema Dataframe

    Args:
        :param raw: the spark session
        :param event_class one of "open", "delivery", "complaint", "send", "reject", "bounce", "click"

    Returns:
        * A transformed dataframe

    """
    subtype_udf = udf(get_event_data, event_schema)
    filename_udf = udf(get_file_info, file_schema)

    merge_df = raw.withColumnRenamed(event_class, "event")

    # flatten it out a bit and conform any class specific names e.g. bounce --> event
    df = merge_df \
        .withColumn("Headers", col("mail.commonHeaders")) \
        .withColumn("Tags", col("mail.tags")) \
        .withColumn("FileInfo", filename_udf(input_file_name())) \
        .withColumn("EventInfo", subtype_udf(merge_df['event']))

    # do the main transform, add some tracking info and return
    return df.select(lower(col("eventType")).alias("event_class"),
                     lit(current_timestamp()).alias("etl_time"),
                     col("mail.timestamp").alias("timestamp"),
                     col("mail.source").alias("source"),
                     col("mail.sendingAccountId").alias("account"),
                     col("mail.messageId").alias("message_id"),
                     "Headers.*",
                     # "Tags.*",
                     "EventInfo.*",
                     col("event").alias("event_data"),
                     "FileInfo.*").withColumn("completion_time", to_timestamp(col("event_time")))


def get_event_data(event: str) -> Row:
    """ parse out the specific event (e.g. 'bounce') from a json string

    :param event: a json string of event data
    :return: a common 'event' Row

    """
    elem = json.loads(event)
    end_type = ""
    subtype = ""
    timestamp = ""
    recipients = []
    if "timestamp" in elem:
        timestamp = elem["timestamp"]

    if "bounceType" in elem:
        end_type = str(elem["bounceType"]).lower()
        subtype = str(elem["bounceSubType"]).lower()
        recipients = elem["bouncedRecipients"]
    if "complaintSubType" in elem:
        end_type = str(elem["complaintSubType"]).lower()
        subtype = str(elem["complaintFeedbackType"]).lower()
        recipients = elem["complainedRecipients"]

    row = Row('event_time', 'event_type', 'event_subclass', 'event_recipients')(timestamp, end_type, subtype,
                                                                                recipients)
    return row


def get_file_info(filename: str) -> Row:
    """ parse out the file and bucket info

    :param filename: a string filename
    :return: a conformed file Row

    """
    bucket_name = "not-supported"
    file_name = filename
    m = re.search(r"""^s3.\:\/\/([^\/]*)\/(.*)$""", filename)
    if m:
        bucket_name = m.group(1)
        file_name = m.group(2)
    return Row('bucket', 'filename')(bucket_name, file_name)


