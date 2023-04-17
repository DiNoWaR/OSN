from pyspark.sql.functions import *
from util.sparkUtil import *
from part_two import *
import unittest

spark = get_spark_session()
test_data = spark.read.option("header", True).option("inferSchema", True)\
    .csv("../test_input/sf-airbnb-clean.csv")

class TestPartTwo(unittest.TestCase):

    def test_part_two_1(self):
        actual_df = get_min_max_price_and_count(test_data)
        rows = actual_df.collect()
        actual_data = [(row.min_price, row.max_price, row.row_count) for row in rows]
        expected_data = [(1200, 5500, 5)]
        self.assertEqual(actual_data, expected_data)

    def test_part_two_2(self):
        actual_df = get_bedrooms_bathrooms(test_data)
        rows = actual_df.collect()
        actual_data = [(row.avg_bathrooms, row.avg_bedrooms) for row in rows]
        expected_data = [(2.0, 2.5)]
        self.assertEqual(actual_data, expected_data)

    def test_part_two_3(self):
        actual_df = get_capacity(test_data)
        rows = actual_df.collect()
        actual_data = [row.accommodates for row in rows]
        expected_data = [1]
        self.assertEqual(actual_data, expected_data)


if __name__ == '__main__':
    unittest.main()
