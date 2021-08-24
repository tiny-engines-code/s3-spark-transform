import json
import re

from pyspark.sql import *
from pyspark.sql.functions import col, lit, lower, input_file_name, udf, current_timestamp, to_timestamp, count, when
from pyspark.sql.types import *

__all__ = ["DataFrameReader", "DataFrameWriter"]

# --------------------------------------------------
#  Schemas
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


# --------------------------------------------------
#  get_event_data
#       - Parse the speocific event json for any event class (eg.g bounce, open...)
#
def get_event_data(event_class, event):
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


# --------------------------------------------------
#  get_file_info
#       - Put the file info on the df for future validations
#
def get_file_info(filename):
    bucket_name = "not-supported"
    file_name = filename
    m = re.search(r"""^s3.\:\/\/([^\/]*)\/(.*)$""", filename)
    if m:
        bucket_name = m.group(1)
        file_name = m.group(2)
    return Row('bucket', 'filename')(bucket_name, file_name)


# --------------------------------------------------
#  conform_events
#       - Merge open, bounce, send, rejects all into a single event schema
#
def conform_events(df, threshold=0):

    columns = ["open", "delivery", "complaint", "send", "reject", "bounce", "click"]
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in columns]).collect()[0].asDict()

    event_name = ""
    for i, v in null_counts.items():
        if v == 0:
            event_name = i

    to_drop = [k for k, v in null_counts.items() if v > 0]
    df = df.drop(*to_drop)

    if len(event_name) == 0:
        df2 = df.withColumn("event", lit("{}"))
        return df2
    else:
        df2 = df.withColumnRenamed(event_name, "event")
        return df2


# --------------------------------------------------
#  transform_ses_records
#       - Main transform method
#
#  input:  any ses events df
#  output: flat conformed records df
#
def transform_ses_records(raw: DataFrame) -> DataFrame:
    subtype_udf = udf(get_event_data, event_schema)
    filename_udf = udf(get_file_info, file_schema)

    conformed_df = conform_events(raw)

    df = conformed_df \
        .withColumn("Headers", col("mail.commonHeaders")) \
        .withColumn("Tags", col("mail.tags")) \
        .withColumn("FileInfo", filename_udf(input_file_name())) \
        .withColumn("EventInfo", subtype_udf(conformed_df['eventType'], conformed_df['event']))

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
