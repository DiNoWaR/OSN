from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def read_iris_dataset(spark, path):
    schema = "sepal_length DOUBLE, sepal_width DOUBLE, petal_length DOUBLE, petal_width DOUBLE, class STRING"
    return spark.read.schema(schema).csv(path)


def make_prediction(iris_dataframe, pred_data):
    indexer = StringIndexer(inputCol='class', outputCol='label')
    indexed_df = indexer.fit(iris_dataframe).transform(iris_dataframe)

    assembler = VectorAssembler(inputCols=['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], outputCol='features')
    dataset = assembler.transform(indexed_df).select('features', 'label')

    log_regression = LogisticRegression(featuresCol='features', labelCol='label', tol=0.0001)
    model = log_regression.fit(dataset)

    return model.transform(assembler.transform(pred_data)) \
        .select(col('prediction').alias('class'))









