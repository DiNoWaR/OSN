from util.sparkUtil import spark
from pyspark.sql.functions import *
from pyspark.sql.window import Window

RAW_CSV_PATH = "/Users/denisvasilyev/Documents/Projects/OSN/input/groceries.csv"
UNIQUE_PRODUCTS_PATH = "/Users/denisvasilyev/Documents/Projects/OSN/out/out_1_2a.txt"
TOTAL_COUNTS_PRODUCTS_PATH = "/Users/denisvasilyev/Documents/Projects/OSN/out/out_1_2b.txt"
TOP_FIVE_PURCHASED_PATH = "/Users/denisvasilyev/Documents/Projects/OSN/out/out_1_3.txt"


def read_groceries_and_preprocess(path):
    raw_dataframe = spark.read.text(path)
    return raw_dataframe \
        .select(split('value', ',').alias("product")) \
        .select(explode('product').alias("product")) \
        .filter(col('product').isNotNull() & (length(col("product")) > 0))


def read_airbnb(path):
    raw_dataframe = spark.read.parquet(path)
    return raw_dataframe


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
    window = Window().orderBy(col('total').desc())

    top_five = dataframe.groupBy('product') \
        .agg(count("*").alias('total')) \
        .withColumn('row', row_number().over(window)) \
        .filter(col('row') <= 5) \
        .drop('row')

    write_dataframe(top_five, TOP_FIVE_PURCHASED_PATH)


def bgg(dataframe):
    return dataframe.agg(min('price').alias('min_price'), max('price').alias('max_price'), count("*").alias('row_count'))


def bgg2(dataframe):
    return dataframe.filter((col('price') > 5000) & (col('review_scores_value') == 10)) \
        .agg(avg('bathrooms').alias('avg_bathrooms'), avg('bedrooms').alias('avg_bedrooms'))


def bgg3(dataframe):
    window = Window().orderBy(col('price').asc(), col('review_scores_rating').desc())

    return dataframe.withColumn('row', row_number().over(window)) \
        .filter(col('row') == 1) \
        .drop('row')


if __name__ == "__main__":
    # dataframe = read_groceries_and_preprocess(RAW_CSV_PATH)
    # save_unique_products(dataframe)
    # save_count_of_all_products(dataframe)
    # save_top_five(dataframe)
    # df = read_airbnb("/Users/denisvasilyev/Documents/Projects/OSN/input/sf-airbnb-clean.parquet")
    df = read_airbnb("/Users/denisvasilyev/Documents/Projects/OSN/input/sf-airbnb-clean.parquet")
    bgg3(df).show(500, False)