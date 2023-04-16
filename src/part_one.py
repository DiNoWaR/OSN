from pyspark.sql.functions import *
from pyspark.sql.window import Window


def read_groceries_and_preprocess(spark, path):
    raw_dataframe = spark.read.text(path)
    return raw_dataframe \
        .select(split('value', ',').alias("product")) \
        .select(explode('product').alias("product")) \
        .filter(col('product').isNotNull() & (length(col("product")) > 0))


def get_unique_products(dataframe):
    return dataframe.distinct() \
        .withColumn("product", concat(lit('product '), col('product'))) \


def count_all_products(dataframe):
    return dataframe.select(expr("count(*)").alias('total')) \
        .withColumn("total", concat(lit('Count: '), col('total'))) \


def get_top_give_products(dataframe):
    window = Window().orderBy(col('total').desc())

    return dataframe.groupBy('product') \
        .agg(count("*").alias('total')) \
        .withColumn('row', row_number().over(window)) \
        .filter(col('row') <= 5) \
        .drop('row')

