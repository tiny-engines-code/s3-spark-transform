import re
import uuid
from datetime import datetime

from pyspark import SparkConf
from pyspark.sql import *
from pyspark.sql.functions import col, lit, lower, input_file_name, udf, current_timestamp, to_timestamp
from pyspark.sql.types import *

__all__ = ["DataFrameReader", "DataFrameWriter"]

return_codes = {
    '5.1.1': "unreachable",  # "DoesNotExist",
    '5.3.0': "unreachable",  # "mailbox unavailable",
    '5.7.1': "access denied",
    '4.3.0': "unreachable",  # "MailboxUnavailable",
    '5.5.0': "unreachable",  # "MailboxUnavailable",
    '5.4.316': "unreachable",  # "Expired",
    '5.1.2': "unreachable",  # "UnknownServer",
    '4.0.0': "mailboxfull",
    '4.2.2': "mailboxfull",
    '4.5.2': "mailboxfull",
    '4.4.7': "unreachable",  # "NoResponse",
    '5.4.4': "unreachable",  # "InvalidDomain"
}

header_schema = StructType([
    StructField("send_id", StringType(), False)])

event_schema = StructType([
    StructField("end_time", StringType(), False),
    StructField("end_type", StringType(), False),
    StructField("end_sub_type", StringType(), False)
])

file_schema = StructType([
    StructField("bucket", StringType(), False),
    StructField("filename", StringType(), False)
])


def create_schema(element: str) -> StructType:
    schema_map = {
        "delivery": StructType([
            StructField('timestamp', StringType(), False),
        ]),
        "send": StructType([
            StructField('timestamp', StringType(), False),
        ]),
        "reject": StructType([
            StructField('timestamp', StringType(), False),
        ]),
        "open": StructType([
            StructField('timestamp', StringType(), False),
        ]),
        "bounce": StructType([
            StructField('feedbackId', StringType(), True),
            StructField('bounceType', StringType(), True),
            StructField('bounceSubType', StringType(), True),
            StructField('timestamp', StringType(), True),
            StructField('reportingMTA', StringType(), True),
            StructField('bouncedRecipients', ArrayType(
                StructType([
                    StructField('emailAddress', StringType(), True),
                    StructField('action', StringType(), True),
                    StructField('status', StringType(), True),
                    StructField('diagnosticCode', StringType(), True)])))
        ]),
        "complaint": StructType([
            StructField('feedbackId', StringType(), True),
            StructField('complaintFeedbackType', StringType(), True),
            StructField('complaintSubType', StringType(), True),
            StructField('timestamp', StringType(), True),
            StructField('complainedRecipients', ArrayType(
                StructType([
                    StructField('emailAddress', StringType(), True),
                    StructField('action', StringType(), True),
                    StructField('status', StringType(), True)])))
        ])
    }

    schema = None
    if element in schema_map:
        schema = schema_map[element]

    if schema is None:
        print(f"No schema called {element}")
        raise Exception(f"No schema called {element}")

    root_schema = StructType([
        StructField('eventType', StringType(), True),
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

    root_schema.add(StructField(element, schema, False))
    return root_schema


def get_event_data(elem):
    end_type = ""
    subtype = ""
    timestamp = ""
    recipients = []
    if hasattr(elem, 'timestamp'):
        timestamp = elem.timestamp
    if hasattr(elem, 'bounceType'):
        end_type = str(elem.bounceType).lower()
        subtype = str(elem.bounceSubType).lower()
        recipients = elem.bouncedRecipients
    if hasattr(elem, 'complaintSubType'):
        end_type = str(elem.complaintSubType).lower()
        subtype = str(elem.complaintFeedbackType).lower()

    for row in recipients:
        if subtype == 'general':
            m = re.search(r"(\d\.\d\.\d)", row.diagnosticCode)
            if m:
                found = m.group(1)
                if found in return_codes:
                    subtype = return_codes[found]

    return Row('end_time', 'end_type', 'end_sub_type', 'diagnostic') \
        (timestamp, end_type, subtype)


def get_send_id(arr):
    # print(f"ARRAY : {arr}")
    send_id = ""
    for row in arr:
        if "X-sendId" == row.name:
            send_id = row.value
    return Row('send_id')(send_id)


def get_file_info(filename):
    bucket_name = "cherry"
    file_name = "bomb"
    m = re.search(r"""^s3.\:\/\/([^\/]*)\/(.*)$""", filename)
    if m:
        bucket_name = m.group(1)
        file_name = m.group(2)
    return Row('bucket', 'filename')(bucket_name, file_name)


def write_jdbc(df, etl_id):
    db_target_url = "jdbc:postgresql://cds-kafka-sink.cfdzoccamj4b.us-east-2.rds.amazonaws.com:5432/postgres"
    db_target_properties = {"user": "nike", "password": "justdoit"}

    df.write.jdbc(url=db_target_url, table="public.processed_streaming", mode="append", properties=db_target_properties)
    pass


def transform_df(raw: DataFrame) -> (DataFrame, DataFrame):
    header_udf = udf(get_send_id, header_schema)
    subtype_udf = udf(get_event_data, event_schema)
    filename_udf = udf(get_file_info, file_schema)

    merge_df = raw.withColumnRenamed("bounce", "event") \
        .withColumnRenamed("open", "event") \
        .withColumnRenamed("delivery", "event") \
        .withColumnRenamed("complaint", "event") \
        .withColumnRenamed("send", "event") \
        .withColumnRenamed("reject", "event")

    df = merge_df\
        .withColumn("etl_time", lit(current_timestamp())) \
        .withColumn("event_time", to_timestamp(col("mail.timestamp"))) \
        .withColumn("Output", header_udf(merge_df["mail.headers"])) \
        .withColumn("FileInfo", filename_udf(input_file_name())) \
        .withColumn("EventInfo", subtype_udf(merge_df['event']))

    df2 = df.select(lower("eventType").alias("event_type"),
                    col("mail.messageId").alias("message_id"),
                    "etl_time",
                    "event_time",
                    "Output.*",
                    "EventInfo.*",
                    "FileInfo.*")\
        .withColumn("completion_time", to_timestamp(col("end_time")))

    file_df = df2.select("bucket", "filename", lit("ses").alias("topic")).distinct()

    mail_df = df2.select("event_type", "message_id", "send_id", "event_time", "completion_time", "end_type",
                         "end_sub_type", "etl_time")

    return mail_df, file_df


def read_s3_json(spark: SparkSession, event_type: str, input_folder: str, mail_folder: str, files_folder: str):
    schema = create_schema(event_type)

    raw = spark.read.json(input_folder, schema)

    mail_df, file_df = transform_df(raw)
    mail_df.show(10, False)
    file_df.show(10, False)

    x = datetime.now()
    sub_folder = f"year={x.year}/month={x.month:02}/day={x.day:02}/hour={x.hour:02}//{str(uuid.uuid4())}"
    mail_df.write.json(mail_folder + sub_folder)

    print(f"processed {mail_df.count()} records")


def read_s3_stream(spark: SparkSession, event_type: str, input_folder: str, mail_folder: str, files_folder: str,
                   checkpoint_path: str):
    schema = create_schema(event_type)

    raw = spark \
        .readStream.json(input_folder, schema)

    mail_df, file_df = transform_df(raw)

    x = datetime.now()
    sub_folder = f"year={x.year}/month={x.month:02}/day={x.day:02}/hour={x.hour:02}/{event_type}"

    # Start running the query in update output mode that prints the running counts to the console
    mail_query = mail_df \
        .writeStream \
        .option("path", mail_folder + sub_folder) \
        .option("checkpointLocation", f"{checkpoint_path}/mail") \
        .format('json') \
        .start()

    file_query = file_df.writeStream.foreachBatch(write_jdbc).start()

    mail_query.awaitTermination()
    file_query.awaitTermination()


def process_s3_directory(spark: SparkSession, eventType: str):
    input_folder = f"s3a://cds-data-temp/ses/mail/{eventType}/2021/*/*/*"
    mail_folder = "s3a://cds-data-temp/data/ses/mail/"
    files_folder = "s3a://cds-data-temp/data/ses/files/"
    checkpoint_path = f"s3a://cds-data-temp/checkpoints/ses/{eventType}"

    # read_s3_json(spark=spark, event_type=eventType,
    #              input_folder=input_folder,
    #              mail_folder=mail_folder,
    #              files_folder=files_folder)

    read_s3_stream(spark=spark, event_type=eventType,
                   input_folder=input_folder,
                   mail_folder=mail_folder,
                   files_folder=files_folder, checkpoint_path=checkpoint_path)


if __name__ == "__main__":

    local = True
    conf = SparkConf()
    conf.setAppName('SES Transformer')

    if local:
        conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
        conf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
                 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    else:
        conf.set("spark.sql.parquet.fs.optimized.committer.optimization-enabled", True)
        conf.set("spark.executor.memory", "1g")
        conf.set("spark.dynamicAllocation.enabled", True)
        conf.set("spark.sql.parquet.fs.optimized.committer.optimization-enabled", True)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # Use a smaller number for the shuffle partitions
    spark.conf.set("spark.sql.shuffle.partitions", 1)

    if local:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIA26BBQRVXZ5Q5CYG6")
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger('CDS-ses-transform')

    process_s3_directory(spark, "bounce")
