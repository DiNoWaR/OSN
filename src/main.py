from util.sparkUtil import spark
from pyspark.sql.functions import *

RAW_CSV_PATH = "/Users/denisvasilyev/Documents/Projects/OSN/input/groceries.csv"
UNIQUE_PRODUCTS_PATH = "/Users/denisvasilyev/Documents/Projects/OSN/out/out_1_2a.txt"
TOTAL_COUNTS_PRODUCTS_PATH = "/Users/denisvasilyev/Documents/Projects/OSN/out/out_1_2b.txt"
TOP_FIVE_PURCHASED_PATH = "/Users/denisvasilyev/Documents/Projects/OSN/out/out/out_1_3.txt"


def read_raw_file_and_preprocess(path):
    raw_dataframe = spark.read.text(path)
    return raw_dataframe \
        .select(split('value', ',').alias("product")) \
        .select(explode('product').alias("product")) \
        .filter(col('product').isNotNull() & (length(col("product")) > 0))


def write_dataframe(dataframe, output_path):
    dataframe.coalesce(1) \
        .write \
        .option("header", "false") \
        .mode("overwrite") \
        .csv(output_path)


def save_unique_products(dataframe):
    unique_products = dataframe.distinct() \
        .withColumn("product", concat(lit('product '), col('product'))) \

    write_dataframe(unique_products, UNIQUE_PRODUCTS_PATH)


def save_count_of_all_products(dataframe):
    counts = dataframe.select(expr("count(*)").alias('total')) \
        .withColumn("total", concat(lit('Count: '), col('total'))) \

    write_dataframe(counts, TOTAL_COUNTS_PRODUCTS_PATH)


def save_top_five(dataframe):
    top_five = dataframe.groupBy('product') \
        .agg(count("*").alias('total')) \
        .orderBy(col('total').desc()) \

    write_dataframe(top_five, TOP_FIVE_PURCHASED_PATH)


if __name__ == "__main__":
    dataframe = read_raw_file_and_preprocess(RAW_CSV_PATH)
    save_unique_products(dataframe)
    save_count_of_all_products(dataframe)
    save_top_five(dataframe)
