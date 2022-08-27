from pyspark.sql import SparkSession
from config import *


class initSparkApp():
    """
    # Keep Memory constant regardless of input
    # Delta Lake Support
    """

    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName(SPARK_APP_NAME) \
            .config("spark.sql.files.maxPartitionBytes", MAXPARTITIONBYTES) \
            .getOrCreate()

    def initSparkSession(self):
        return self.spark

    def stopSparkSession(self):
        self.spark.stop()
