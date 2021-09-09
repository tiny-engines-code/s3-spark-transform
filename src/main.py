""" Main file - transform all SES event files from a given set of directories to a single schema

Textbook example that recursively reads all ALL SES event files from a given root directory, reads the specific schema
for each event class (e.g. bounce, open, ...) and transforms to a single schema

    :main: is a simple example starter
    :etl: calls the main processor for each event class
    :process_event_directory: main worker for a single event class

"""

import os

from src.configure import get_spark
from src.readers import read_json_streaming, read_json_file
from src.writers import write_sink

try:
    from pyspark import SparkConf
    from pyspark.sql import *
except ImportError as e:
    print("Error importing Spark Modules", e)


def etl(pc: SparkSession, input_path: str, output_path: str, checkpoint_path: str =None):
    """Cycles through all event classes and process each.

    Processes one event name at a time but creates a common schema for all

    Args:
        :param pc: the spark session
        :param input_path: root directory for the input files
        :param output_path: where to store the new conformed files
        :param checkpoint_path: where Spark Structure Streaming should keep track of our progress

    Returns:
        * create output files from the files in the input_path
        * Spark creates a checkpoint directory if it does not exist
        * We create an output directory if it does not exist
    """
    for i in ["open", "delivery", "complaint", "send", "reject", "bounce", "click"]:
        process_event_directory(pc, i, input_path, output_path, checkpoint_path)


def process_event_directory(pc: SparkSession, event_class: str, input_path: str, output_path: str,
                            checkpoint_path: str = None):
    """Process one event class.

    Processes one event name

    Args:
        :param pc: the spark session
        :param event_class: one of "open", "delivery", "complaint", "send", "reject", "bounce", "click"
        :param input_path: root directory for the input files
        :param output_path: where to store the new conformed files
        :param checkpoint_path: where Spark Structure Streaming should keep track of our progress

    Returns:
        * create output files from the files in the input_path
        * Spark creates a checkpoint directory if it does not exist
        * We create an output directory if it does not exist

    """
    input_path = os.path.join(input_path, event_class + "/")
    output_path = os.path.join(output_path, event_class + "/")

    # Hack, Hack - since this is an example I'm including a few different sinks that
    # might be useful during development
    # todo - put postgres in a docker file rather than calling my local version
    #
    if checkpoint_path is not None:
        mail_df = read_json_streaming(pc=pc, event_class=event_class, input_path=input_path)
        write_sink(mail_df, output_path, ["stream"], checkpoint_path)
    else:
        mail_df = read_json_file(pc=pc, event_class=event_class, input_path=input_path)
        if output_path.startswith("jdbc"):
            write_sink(mail_df, output_path, ["jdbc"])
        elif checkpoint_path is None:
            write_sink(mail_df, output_path, ["file"])
        else:
            write_sink(mail_df, output_path, ["stream"])


if __name__ == "__main__":

    spark = get_spark()
    local_path = "/Users/clome1/Source/projects/tiny-engines/s3-spark-transform"

    # etl from local to postgres - if you want to work with the data in postgres
    # etl(spark, input_path=f"{local_path}/input/", output_path="jdbc")

    # etl from local batch to local
    # etl(spark, input_path=f"{local_path}/input/", output_path=f"{local_path}/output_local/")

    # etl from local streaming to local streaming
    etl(spark, input_path=f"{local_path}/input/", output_path=f"{local_path}/output_streaming/",
        checkpoint_path=f"{local_path}/checkpoint/")

    # etl(spark, input_path=f"s3a://my_bucket/ses/mail/", output_path="s3a://my_bucket/data/ses/mail/",
    #     checkpoint_path="s3a://my_bucket/checkpoints/ses/")
