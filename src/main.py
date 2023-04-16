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

    # Part 1
    groceries_df = read_groceries_and_preprocess(spark, GROCERIES_INPUT_PATH)

    unique_products = get_unique_products(groceries_df)
    all_products_count = count_all_products(groceries_df)
    top_five_products = get_top_give_products(groceries_df)

    for df, path in {
        unique_products: UNIQUE_PRODUCTS_OUTPUT_PATH,
        all_products_count: TOTAL_COUNTS_PRODUCTS_OUTPUT_PATH,
        top_five_products: TOP_FIVE_PURCHASED_OUTPUT_PATH
    }.items():
        write_dataframe(df, path, headers=False)

    # Part 2
    airbnb_df = read_airbnb_set(spark, AIRBNB_INPUT_PATH)

    min_max_count_df = get_min_max_price_and_count(airbnb_df)
    bedrooms_bathrooms_df = get_bedrooms_bathrooms(airbnb_df)
    capacity_df = get_capacity(airbnb_df)

    for df, path in {
        min_max_count_df: MIN_MAX_COUNTS_OUTPUT_PATH,
        bedrooms_bathrooms_df: BEDROOMS_BATHROOMS_OUTPUT_PATH,
        capacity_df: CAPACITY_OUTPUT_PATH
    }.items():
        write_dataframe(df, path, headers=True)

    # Part 4
    iris_df = read_iris_dataset(spark, IRIS_INPUT_PATH)

    pred_data = spark.createDataFrame(
        [(5.1, 3.5, 1.4, 0.2),
         (6.2, 3.4, 5.4, 2.3)],
        ["sepal_length", "sepal_width", "petal_length", "petal_width"])

    pred_df = make_prediction(iris_df, pred_data)
    write_dataframe(pred_df, IRIS_PRED_OUTPUT_PATH, headers=True)
