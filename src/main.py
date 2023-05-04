from util.sparkUtil import *
from part_one import *
from part_two import *
from part_four import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

GROCERIES_INPUT_PATH = "../input/groceries.csv"
UNIQUE_PRODUCTS_OUTPUT_PATH = "../out/out_1_2a.txt"
TOTAL_COUNTS_PRODUCTS_OUTPUT_PATH = "../out/out_1_2b.txt"
TOP_FIVE_PURCHASED_OUTPUT_PATH = "../out/out_1_3.txt"


AIRBNB_INPUT_PATH = "../input/sf-airbnb-clean.parquet"
MIN_MAX_COUNTS_OUTPUT_PATH = "../out/out_2_2.txt"
BEDROOMS_BATHROOMS_OUTPUT_PATH = "../out/out_2_3.txt"
CAPACITY_OUTPUT_PATH = "../out/out_2_4.txt"

IRIS_INPUT_PATH = "../input/iris.csv"
IRIS_PRED_OUTPUT_PATH = "../out/out_3_2.txt"

if __name__ == "__main__":
    spark = get_spark_session()

    date_regex = '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\s*$'

    df = spark.read.option('header', 'true').csv("/Users/denisvasilyev/Documents/Projects/OSN/input/nyc-jobs.csv")
    df.select('Agency', 'Job Category', 'Posting Date', 'Business Title', 'Salary Range From', 'Salary Range To',
              'Preferred Skills', 'Minimum Qual Requirements') \
        .na.fill(value='null') \
        .filter(col('Job Category') != 'null') \
        .filter(col('Business Title') != 'null') \
        .filter(col('Salary Range From') != 'null') \
        .filter(col('Salary Range To') != 'null') \
        .filter(col('Preferred Skills') != 'null') \
        .filter(col('Posting Date').rlike(date_regex)) \
        .sample(0.02) \
        .coalesce(1) \
        .write \
        .option("header", 'true') \
        .mode("overwrite") \
        .csv("../out/mmmm.csv")

