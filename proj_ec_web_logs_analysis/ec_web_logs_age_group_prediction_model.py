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
    feature_prep = logs.select("product_category_id", "device_type", "connect_type", "age_group") \
                       .where(~isnull("age_group"))

    feature_prep = StringIndexer(inputCol="age_group", outputCol="age_group_indexed").fit(feature_prep).transform(feature_prep)

    final_data = VectorAssembler(inputCols=["product_category_id", "device_type", "connect_type"],
                                 outputCol="features").transform(feature_prep)

    # Split data into train and test sets
    train_data, test_data = final_data.randomSplit([0.7, 0.3])

    # Model training
    classifier = RandomForestClassifier(featuresCol="features", labelCol="age_group_indexed", numTrees=20, maxDepth=10)
    model = classifier.fit(train_data)

    # Transform the test data using the model to get predictions
    predicted_test_data = model.transform(test_data)

    # Evaluate the model performance
    evaluator_accuracy = MulticlassClassificationEvaluator(labelCol='age_group_indexed',
                                                           predictionCol='prediction',
                                                           metricName='accuracy')
    print("Accuracy: {}", evaluator_accuracy.evaluate(predicted_test_data))


    confusion_matrix_info = predicted_test_data.select("age_group_indexed", "prediction")\
                                               .groupBy("age_group_indexed", "prediction")\
                                               .count()

    confusion_matrix_info.orderBy("age_group_indexed", "prediction").show()
    confusion_matrix_info.orderBy("prediction", "age_group_indexed").show()

    evaluator_f1 = MulticlassClassificationEvaluator(labelCol='age_group_indexed',
                                                     predictionCol='prediction',
                                                     metricName='f1')
    print("F1 score: {}", evaluator_f1.evaluate(predicted_test_data))

    # Save the model
    model.save("hdfs://devenv/user/spark/spark_mllib_101/ec_web_logs_analysis/model_age_group_prediction/")


    # +---------+-----------------+                                                   
    # |age_group|age_group_indexed|
    # +---------+-----------------+
    # | under 20|              2.0|
    # |  over 50|              3.0|
    # |    21-35|              0.0|
    # |    36-50|              1.0|
    # +---------+-----------------+