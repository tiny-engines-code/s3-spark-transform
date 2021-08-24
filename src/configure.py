try:
    from pyspark import SparkConf
    from pyspark.sql import *
except ImportError as e:
    print("Error importing Spark Modules", e)


# create a SparkSession
#
# are we running spark locally for development or on EMR
# are we writing to s3 or local file system
#
def get_spark(local_spark: bool = True, s3fs: bool = False) -> SparkSession:
    conf = SparkConf()
    conf.setAppName('SES Transformer')
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')

    # conf.set("spark.jars", "/usr/local/Cellar/apache-spark/3.1.2/libexec/jars/postgresql-42.2.23.jar")
    # conf.set('spark.jars.packages', '/usr/local/Cellar/apache-spark/3.1.2/libexec/jars')

    # if local_spark and s3fs:
    #     conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
    #     conf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
    #              'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    # else:
    #     conf.set("spark.sql.parquet.fs.optimized.committer.optimization-enabled", True)
    #     conf.set("spark.executor.memory", "1g")
    #     conf.set("spark.dynamicAllocation.enabled", True)
    #     conf.set("spark.sql.parquet.fs.optimized.committer.optimization-enabled", True)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 1)

    # Use a smaller number for the shuffle partitions
    # spark.conf.set("spark.sql.shuffle.partitions", 1)

    # if local_spark and s3fs:
    #     spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "xxxxx")
    #     spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "xxxxx")
    #
    # log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    # log = log4jLogger.LogManager.getLogger('ses-transform')
    return spark
