import json
import re
import time
import uuid
from datetime import datetime
from uuid import uuid4

from pyspark import SparkConf, RDD
from pyspark.sql import *
from pyspark.sql.functions import col, lit, input_file_name, udf, explode
from pyspark.sql.types import *

__all__ = ["DataFrameReader", "DataFrameWriter"]

# header fields we want to get, and field name translations
header_map = {
    'X-sendid': 'send_id',
    'from': 'sender',
    'to': 'recipient'
}

return_codes = {
    '5.1.1':   "unreachable", # "DoesNotExist",
    '5.3.0':   "unreachable", # "mailbox unavailable",
    '5.7.1':   "access denied",
    '4.3.0':   "unreachable", # "MailboxUnavailable",
    '5.5.0':   "unreachable", # "MailboxUnavailable",
    '5.4.316': "unreachable", # "Expired",
    '5.1.2':   "unreachable", # "UnknownServer",
    '4.0.0':   "mailboxfull",
    '4.5.2':   "mailboxfull",
    '4.4.7':   "unreachable", # "NoResponse",
    '5.4.4':   "unreachable", # "InvalidDomain"
}


def set_enhanced_type(ses_mail: dict, json_string : str, sub_type : str) -> str:
    ses_mail['event_code'] = field_value = ""
    ses_mail['event_sub_category'] = sub_type.lower()
    m = re.search(r"(\"status\":\s*\")([^\"]*)", json_string)
    if len(m.groups()) >= 2:
        field_value = m.group(2)
        ses_mail['event_code'] = field_value
    if sub_type in ["General", "Undetermined"]:
        if field_value in return_codes:
            ses_mail['event_code'] = field_value
            ses_mail['event_sub_category'] = return_codes[field_value]
            ses_mail['diagnostic_code'] = get_diagnostic_code(json_string, "")


def get_diagnostic_code(json : str, default_value : str) -> str:
    m = re.search(r"(\"diagnosticCode\":\s*\")([^\"]*)", json)
    if m and len(m.groups()) >= 2:
        s = m.group(2)
        d = re.search(r"(.{30})([^\.]*)", s)
        if d and len(d.groups()) >= 2:
            return d.group(1) + d.group(2)

        return s
    return default_value[: min(len(default_value), 50)]


def get_field(ses: dict, key: str, defaultValue: any):
    if key in ses:
        return ses[key]
    return defaultValue


def get_timestamp(ses: dict, key: str, default_value: LongType):
    if key in ses:
        date_string = str(ses[key])
        #return date_string
        try:
            epoch_millis_str = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S.%f")
            epoch_millis= epoch_millis_str.timestamp()
        except Exception as e:
            print(e)
        return epoch_millis

    return default_value


def complaint_info(ses_mail: dict, element: dict):
    basic_info(ses_mail, element)
    ses_mail['event_category'] = get_field(element, "complaintFeedbackType", "")
    ses_mail['event_sub_category'] = get_field(element, "complaintSubType", "")


def bounce_info(ses_mail: dict, element: dict):
    basic_info(ses_mail, element)
    ses_mail['event_category'] = get_field(element, "bounceType", "").lower()
    sub_type = get_field(element, "bounceSubType", "")
    json_string = json.dumps(element)

    set_enhanced_type(ses_mail, json_string, sub_type)


def basic_info(ses_mail: dict, element: dict):
    ses_mail['end_time'] = get_timestamp(element, "timestamp", -1)
    ses_mail['event_record'] = json.dumps(element)


def transfer_event_info(ses_mail: dict, ses: dict, event_type: str):
    try:
        # set event-type specific data
        if "bounce" == event_type: bounce_info(ses_mail, ses["bounce"])
        if "complaint" == event_type: complaint_info(ses_mail, ses["complaint"])
        if "open" == event_type: basic_info(ses_mail, ses["open"])
        if "delivery" == event_type: basic_info(ses_mail, ses["delivery"])
    except Exception as e:
        # log.error("Bad specific element "+str(e))
        ses_mail['error'] = str(e)


def load_json(line: str) -> dict:
    try:
        return json.loads(line)
    except Exception as e:
        # log.error("Bad json --> "+line)
        return {'error': str(e)}


def ses_parser(line: str) -> dict:
    try:
        return ses_formatter(line)
    except Exception as e:
        # log.error("Bad format --> "+line)
        return {}


def request_formatter(line: tuple) -> dict:
# ses = load_json(line)
    requests=[]
    pattern = """(\"request_id"\s*:\s*\")([^\"]*)"""  # find 'an' either with or without a following word character

    filename = line[0]
    text=line[1]

    for match in re.findall(pattern, text):
        requests.append(match[1])

    if len(requests) == 0:
        requests.append("none")

    return Row(
        filename=filename,
        request_id=requests
    )


def ses_formatter(line: str) -> dict:
    null_string = "NaN"
    ses_mail = {
        'event_record': null_string,
        'send_id': null_string,
        'sender': null_string,
        'recipient': null_string
    }

    ses = load_json(line)
    if 'error' in ses:
        print(ses)
        ses_mail.update(ses)
        return ses_mail

    # set some top level fields
    event_type = str(ses["eventType"]).lower()
    ses_mail['event_type'] = event_type

    # transfer mail info
    mail = get_field(ses, 'mail', {})
    ses_mail['message_id'] = get_field(mail, "messageId", null_string)
    ses_mail['mail_time'] = get_timestamp(mail, "timestamp", 0)
    ses_mail['end_time'] = get_timestamp(mail, "timestamp", 0)

    # transfer mail.header info
    headers = get_field(mail, 'headers', [])
    for h in headers:
        name = str(h['name']).lower()
        if name in header_map:
            name = header_map[name]
            value = h['value']
            ses_mail[name] = value

    # transfer mail.tags info
    tags = get_field(mail, "tags", {})
    ses_mail['source_ip'] = "".join(get_field(tags, "ses:source-ip", [null_string]))
    ses_mail['sender'] = "".join(get_field(tags, "ses:from-domain", [null_string]))
    # ses_mail['configuration_set'] = "".join(get_field(tags, "ses:configuration-set", [null_string]))

    # set event-type specific data
    transfer_event_info(ses_mail, ses, event_type)

    # final
    return to_ses_row(ses_mail)


def to_ses_row(ses_mail: dict) -> Row:
    null_string = ""
    # could do this with Row(**ses_mail) - but this is the only place to enforce a full schema
    return Row(
        diagnostic_code=get_field(ses_mail, "diagnostic_code", null_string),
        # configuration_set=get_field(ses_mail, "configuration_set", null_string),
        end_time=get_field(ses_mail, "end_time", 0),
        event_category=get_field(ses_mail, "event_category", null_string),
        # event_code=get_field(ses_mail, "event_code", null_string),
        # event_record=get_field(ses_mail, "event_record", null_string),
        event_sub_category=get_field(ses_mail, "event_sub_category", null_string),
        event_type=get_field(ses_mail, "event_type", null_string),
        mail_time=get_field(ses_mail, "mail_time", 0),
        message_id=get_field(ses_mail, "message_id", null_string),
        recipient=get_field(ses_mail, "recipient", null_string),
        send_id=get_field(ses_mail, "send_id", null_string),
        sender=get_field(ses_mail, "sender", null_string),
        source_ip=get_field(ses_mail, "source_ip", null_string),
    )


def read_local(spark : SparkSession):
    root_dir = "/Users/clome1/Downloads/ses-data/*"
    rows = spark.sparkContext.textFile(root_dir).map(ses_parser).map(lambda x: to_ses_row(x))

    df = rows.toDF() \
        .withColumn("etl_time", lit(time.time())) \
        .withColumn("filename", input_file_name())

    file_df = df.select("message_id", "filename")
    mail_df = df.drop("filename")

    # file_index.show(10, False)
    file_df.show(10, False)
    mail_df.show(10, False)


def read_s3_file(spark : SparkSession):
    input_folder = "s3a://cds-data-temp/ses/mail/bounce/2021/*/*/*"
    mail_folder = "s3a://cds-data-temp/data/ses/data/"
    files_folder = "s3a://cds-data-temp/data/ses/files/"

    raw = spark.sparkContext.textFile(input_folder)
    # for p in raw.top(10):
    #     parser(p)

    rows = raw.map(ses_parser) #.map(lambda_processor x: to_ses_row(x))

    df = rows.toDF() \
        .withColumn("etl_time", lit(time.time()))\
        .withColumn("filename", input_file_name())

    df.show(10,False)

    file_df = df.select("message_id", "filename")
    mail_df = df.drop("filename")

    # file_index.show(10, False)
    file_df.show(10, False)
    mail_df.show(10, False)

    x = datetime.now()
    mail_df.write.parquet(mail_folder+f"{x.year}/{x.month:02}/{x.day:02}/{x.hour:02}/{str(uuid.uuid4())}")
    file_df.write.parquet(files_folder+f"{x.year}/{x.month:02}/{x.day:02}/{x.hour:02}/{str(uuid.uuid4())}")

    print(f"processed {df.count()} records")


def read_s3_requests(spark : SparkSession):
    input_folder = "s3a://cds-requests-archive-test/2021/03/02/*/*/*"
    requests_folder = "s3a://cds-data-temp/data/requests/"

    schema = StructType(
        [
            StructField("filename", StringType(), False),
            StructField("request_id",ArrayType(StringType()),True)
        ]
    )

    rows = spark.sparkContext.wholeTextFiles(input_folder).map(lambda x: request_formatter(x))

    df : DataFrame = spark.createDataFrame(data=rows, schema=schema)
    # df.show(25,False)

    df2 = df.select("filename", explode(df.request_id).alias("request_id"))
    df2.show(20,False)

    print(f"processed {df.count()} records")

if __name__ == "__main__":
    local = True
    conf = SparkConf()
    conf.setAppName('SES Transformer')

    if local:
        conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
        conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    else:
        conf.set("spark.sql.parquet.fs.optimized.committer.optimization-enabled", True)
        conf.set("spark.executor.memory", "1g")
        conf.set("spark.dynamicAllocation.enabled", True)
        conf.set("spark.sql.parquet.fs.optimized.committer.optimization-enabled", True)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    if local:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIA26BBQRVXZ5Q5CYG6")
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger('CDS-ses-transform')
#    test(spark)
#    read_s3_requests(spark)
    read_s3_file(spark)
#    read_local(spark)


