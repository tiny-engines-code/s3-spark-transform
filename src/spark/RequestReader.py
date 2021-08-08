import re
from datetime import datetime
from uuid import uuid4

from pyspark import SparkConf, RDD
from pyspark.sql import *
from pyspark.sql.functions import col, lit, input_file_name, udf, explode
from pyspark.sql.types import *

__all__ = ["DataFrameReader", "DataFrameWriter"]

# get request id's from a file
RE_REQUEST_ID_PATTERN = pattern = """(\"request_id"\s*:\s*\")([^\"]*)"""

# final request index schema
REQUEST_INDEX_SCHEMA = StructType(
    [
        StructField("filename", StringType(), False),
        StructField("request_id", ArrayType(StringType()), True)
    ]
)


# --- list filename:[request_id's] for all of the request_id fields in the file
#
def request_formatter(line: tuple) -> dict:
    requests=[]

    filename = line[0]
    text=line[1]

    for match in re.findall(RE_REQUEST_ID_PATTERN, text):
        requests.append(match[1])

    if len(requests) == 0:
        requests.append("none")

    return Row(
        filename=filename,
        request_id=requests
    )


# --- main process ---
#   read all requests from s3 and create an index of filename: request_id
#   this should allow joining to notification_status by request_id
#
def read_s3_requests(spark : SparkSession, input_folder : str, requests_folder : str):

    rows = spark.sparkContext.wholeTextFiles(input_folder).map(lambda x: request_formatter(x))
    df : DataFrame = spark.createDataFrame(data=rows, schema=REQUEST_INDEX_SCHEMA)
    df_exploded = df.select("filename", explode(df.request_id).alias("request_id"))

    x = datetime.now()
    sub_folder = f"{x.year}/{x.month:02}/{x.day:02}/{x.hour:02}/{str(uuid4())}"
    df_exploded.write.parquet(requests_folder + sub_folder)

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

    read_s3_requests(spark,
                     "s3a://cds-requests-archive-test/2021/03/02/*/*/*",
                     "s3a://cds-data-temp/data/requests/")



