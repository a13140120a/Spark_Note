from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull
from pyspark.ml.feature import VectorAssembler, VectorIndexer, OneHotEncoder, StringIndexer
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Prepare data
    logs = spark.read.parquet("hdfs://devenv/user/spark/spark_mllib_101/ec_web_logs_analysis/data/")

    # Preprocessing and feature engineering
    feature_prep = logs.select("product_category_id", "device_type", "connect_type", "gender") \
                       .where(~isnull("gender"))

    final_data = VectorAssembler(inputCols=["product_category_id", "device_type", "connect_type"],
                                 outputCol="features").transform(feature_prep)

    # Split data into train and test sets
    train_data, test_data = final_data.randomSplit([0.7, 0.3])

    # Model training
    classifier = RandomForestClassifier(featuresCol="features", labelCol="gender", numTrees=10, maxDepth=10)
    model = classifier.fit(train_data)

    # Transform the test data using the model to get predictions
    predicted_test_data = model.transform(test_data)

    # Evaluate the model performance
    evaluator_accuracy = MulticlassClassificationEvaluator(labelCol='gender',
                                                           predictionCol='prediction',
                                                           metricName='accuracy')
    print("Accuracy: {}", evaluator_accuracy.evaluate(predicted_test_data))


    confusion_matrix_info = predicted_test_data.select("gender", "prediction")\
                                               .groupBy("gender", "prediction")\
                                               .count()

    confusion_matrix_info.orderBy("gender", "prediction").show()
    confusion_matrix_info.orderBy("prediction", "gender").show()

    evaluator_f1 = MulticlassClassificationEvaluator(labelCol='gender',
                                                     predictionCol='prediction',
                                                     metricName='f1')
    print("F1 score: {}", evaluator_f1.evaluate(predicted_test_data))

    # Save the model
    model.save("hdfs://devenv/user/spark/spark_mllib_101/ec_web_logs_analysis/model_gender_prediction/")

    # +------+----------+                                                             
    # |gender|prediction|
    # +------+----------+
    # |     1|       1.0|
    # |     0|       0.0|
    # +------+----------+
