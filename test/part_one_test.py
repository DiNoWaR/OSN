from pyspark.sql.functions import *
from util.sparkUtil import *
from part_one import *
import unittest

spark = get_spark_session()

test_data = spark.createDataFrame(
    [("whole milk",), ("other vegetables",), ("rolls/buns",), ("beef",), ("whole milk",), ("chicken",), ("beef",),
     ("other vegetables",), ("rolls/buns",), ("pot plants",), ("beef",), ("chicken",)],
    ["product"])


class TestPartOne(unittest.TestCase):

    def test_part_one_1(self):
        actual_df = get_unique_products(test_data)
        rows = actual_df.collect()

        actual_data = [row.product for row in rows]
        expected_data = ["product whole milk", "product beef", "product chicken",
                         "product rolls/buns", "product pot plants", "product other vegetables"]

        self.assertEqual(sorted(actual_data), sorted(expected_data))

    def test_part_one_2(self):
        actual_df = count_all_products(test_data)
        rows = actual_df.collect()

        actual_data = [row.total for row in rows]
        expected_data = ["Count: 12"]

        self.assertEqual(actual_data, expected_data)

    def test_part_one_3(self):
        actual_df = get_top_give_products(test_data)
        rows = actual_df.collect()

        actual_data = [(row.product, row.total) for row in rows]
        expected_data = [('beef', 3), ('other vegetables', 2), ('rolls/buns', 2), ('chicken', 2), ('whole milk', 2)]

        self.assertEqual(sorted(actual_data), sorted(expected_data))


if __name__ == '__main__':
    unittest.main()

