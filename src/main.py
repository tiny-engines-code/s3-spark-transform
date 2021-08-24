from src.configure_pyspark import get_spark

try:
    from pyspark import SparkConf
    from pyspark.sql import *
except ImportError as e:
    print("Error importing Spark Modules", e)

from readers import process_s3_directory


def etl(pspark: SparkSession, input_path, output_path, checkpoint_path=None):
    for i in ["open", "delivery", "complaint", "send", "reject", "bounce", "click"]:
        process_s3_directory(pspark, i, input_path, output_path, checkpoint_path)


if __name__ == "__main__":

    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
    conf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
             'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

    conf.setAppName('SES Transformer')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 1)

    local_path = "/Users/clome1/Source/projects/tiny-engines/s3-spark-transform"

    # etl from local to postgres - if you want to work with the data in postgres
    # etl(spark, input_path=f"{local_path}/input/", output_path="jdbc")

    # etl from local batch to local
    etl(spark, input_path=f"{local_path}/input/", output_path=f"{local_path}/output_local/")

    # etl from local streaming to local streaming
    # etl(spark, input_path=f"{local_path}/input/", output_path=f"{local_path}/output_streaming/", checkpoint_path=f"{local_path}/checkpoint/")

    # etl(spark, input_path=f"s3a://my_bucket/ses/mail/", output_path="s3a://my_bucket/data/ses/mail/",
    #     checkpoint_path="s3a://my_bucket/checkpoints/ses/")
