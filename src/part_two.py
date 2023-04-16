from pyspark.sql.functions import *
from pyspark.sql.window import Window


def read_airbnb_set(spark, path):
    raw_dataframe = spark.read.parquet(path)
    return raw_dataframe


def get_min_max_price_and_count(dataframe):
    return dataframe.agg(min('price').alias('min_price'), max('price').alias('max_price'), count("*").alias('row_count'))


def get_bedrooms_bathrooms(dataframe):
    return dataframe.filter((col('price') > 5000) & (col('review_scores_value') == 10)) \
        .agg(avg('bathrooms').alias('avg_bathrooms'), avg('bedrooms').alias('avg_bedrooms'))


def get_capacity(dataframe):
    window = Window().orderBy(col('price').asc(), col('review_scores_rating').desc())

    return dataframe.withColumn('row', row_number().over(window)) \
        .filter(col('row') == 1) \
        .select(col('accommodates'))