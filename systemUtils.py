import os
import csv

from os.path import exists
from pyspark.sql.types import *


class systemUtils():
    def __init__(self, spark_session):
        self.spark = spark_session

    def read_csv_to_spark_df(self, file_path):
        return self.spark.read.csv(file_path, header=True)

    def convert_df_column_tpye(self, df, column_name, spark_data_type=StringType()):
        return df.withColumn(column_name, df[column_name].cast(spark_data_type))

    def get_current_path(self):
        cwd = os.getcwd()
        return cwd

    def check_file_exist(self, file_path):
        return exists(file_path)

    def convert_dict_to_sparkDF(self, py_dict, keys=["accountNumber", "balance"]):
        lol = list(map(list, py_dict.items()))
        df = self.spark.createDataFrame(lol, keys)
        return df

    def write_dict_to_csv(self, py_dictionary, headers, output_file_name):
        with open(output_file_name, 'w', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(headers)  # write the header
            for k, v in py_dictionary.items():
                writer.writerow([k, v])

    def write_df_to_csv(self, df, partition_column, output_file_name, output_file_count, mode="overwrite"):
        if partition_column:
            df.write.option("header", True) \
                .partitionBy(partition_column) \
                .mode(mode) \
                .csv(output_file_name)
        else:
            df.coalesce(output_file_count) \
                .write.option("header", True) \
                .mode(mode) \
                .csv(output_file_name)
