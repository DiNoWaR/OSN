from pyspark.sql import SparkSession

def get_spark_session():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("OSN Home") \
        .getOrCreate()


def write_dataframe(dataframe, output_path, headers=True):
    dataframe.coalesce(1) \
        .write \
        .option("header", str(headers).lower()) \
        .mode("overwrite") \
        .csv(output_path)
