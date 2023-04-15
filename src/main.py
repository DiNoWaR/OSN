from util.sparkUtil import spark
from pyspark.sql.functions import *

RAW_CSV_PATH = "/Users/denisvasilyev/Documents/Projects/OSN/input/groceries.csv"
UNIQUE_PRODUCTS_PATH = "/Users/denisvasilyev/Documents/Projects/OSN/out/out_1_2a.txt"


def read_raw_file_and_preprocess(path):
    raw_dataframe = spark.read.text(path)
    return raw_dataframe \
        .select(split('value', ',').alias("product")) \
        .select(explode('product').alias("product"))


def write_dataframe(dataframe, output_path):
    dataframe.coalesce(1) \
        .write \
        .option("header", "false") \
        .mode("overwrite") \
        .text(output_path)


def save_unique_products(dataframe):
    unique_products = dataframe.distinct() \
        .withColumn("product", concat(lit('product '), col('product'))) \

    write_dataframe(unique_products, UNIQUE_PRODUCTS_PATH)


if __name__ == "__main__":
    dataframe = read_raw_file_and_preprocess(RAW_CSV_PATH)
    save_unique_products(dataframe)
