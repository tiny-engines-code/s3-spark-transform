import os

from src.configure import get_spark
from src.readers import read_json_streaming, read_json_file
from src.writers import write_sink

try:
    from pyspark import SparkConf
    from pyspark.sql import *
except ImportError as e:
    print("Error importing Spark Modules", e)


# --------------------------------------------------
#  etl
#       - cycle through all event classes and process each
#
def etl(pspark: SparkSession, input_path, output_path, checkpoint_path=None):
    for i in ["open", "delivery", "complaint", "send", "reject", "bounce", "click"]:
        process_event_directory(pspark, i, input_path, output_path, checkpoint_path)


# --------------------------------------------------
#  process_event_directory
#       - process one event class (eg.g. bounce, sends,..)
#
def process_event_directory(spark_session: SparkSession, event_type: str, source_path: str, sink_path: str,
                            checkpoint_path: str = None):
    source_path = os.path.join(source_path, event_type + "/")
    sink_path = os.path.join(sink_path, event_type + "/")

    # Hack, Hack - since this is an example I'm including a few different sinks that
    # might be useful during development
    # todo - put postgres in a docker file rather than calling my local version
    #
    if checkpoint_path is not None:
        mail_df = read_json_streaming(spark=spark_session, input_folder=source_path)
        write_sink(mail_df, sink_path, ["stream"], checkpoint_path)
    else:
        mail_df = read_json_file(spark=spark_session, input_folder=source_path)
        if sink_path.startswith("jdbc"):
            write_sink(mail_df, sink_path, ["jdbc"])
        elif checkpoint_path is None:
            write_sink(mail_df, sink_path, ["file"])
        else:
            write_sink(mail_df, sink_path, ["stream"])


# --------------------------------------------------
# main
#    - get a spark session and call the real etl processor
#
if __name__ == "__main__":

    spark = get_spark()
    local_path = "/Users/clome1/Source/projects/tiny-engines/s3-spark-transform"

    # etl from local to postgres - if you want to work with the data in postgres
    # etl(spark, input_path=f"{local_path}/input/", output_path="jdbc")

    # etl from local batch to local
    etl(spark, input_path=f"{local_path}/input/", output_path=f"{local_path}/output_local/")

    # etl from local streaming to local streaming
    # etl(spark, input_path=f"{local_path}/input/", output_path=f"{local_path}/output_streaming/",
    #     checkpoint_path=f"{local_path}/checkpoint/")

    # etl(spark, input_path=f"s3a://my_bucket/ses/mail/", output_path="s3a://my_bucket/data/ses/mail/",
    #     checkpoint_path="s3a://my_bucket/checkpoints/ses/")
