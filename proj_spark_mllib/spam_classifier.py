from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler,VectorIndexer, OneHotEncoder, StringIndexer
from pyspark.ml.feature import Tokenizer, RegexTokenizer, StopWordsRemover, CountVectorizer, HashingTF, IDF
from pyspark.ml.linalg import Vector
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import lower, length, size

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Prepare data
    data = spark.read.csv("hdfs://devenv/user/spark/spark_mllib_101/spam_detection/data/sms_messages_with_labels.csv",
                          inferSchema=True,
                          header=True)


    # Preprocessing and feature engineering
    feature_prep = data.select(lower(data["message"]).alias("message"), length(data["message"]).alias("length"), "label")

    feature_prep = RegexTokenizer(inputCol="message", outputCol="words", pattern="\\W+").transform(feature_prep)

    feature_prep = StopWordsRemover(inputCol='words',outputCol='stop_words_removed').transform(feature_prep)

    feature_prep = HashingTF(inputCol="stop_words_removed", outputCol="hashing_tf", numFeatures=4000).transform(feature_prep)

    feature_prep = IDF(inputCol="hashing_tf", outputCol="tf_idf").fit(feature_prep).transform(feature_prep)

    feature_prep = StringIndexer(inputCol='label',outputCol='label_indexed').fit(feature_prep).transform(feature_prep)

    feature_prep = VectorAssembler(inputCols=["tf_idf", "length"],
                           outputCol="features").transform(feature_prep)

    final_data = feature_prep.select("label_indexed", "features")


    # Split data into train and test sets
    train_data, test_data = final_data.randomSplit([0.7,0.3])

    # Model training
    classifier = RandomForestClassifier(featuresCol="features", labelCol="label_indexed", numTrees=100, maxDepth=25)
    model = classifier.fit(train_data)    model = classifier.fit(train_data)


    # Transform the test data using the model to get predictions
    predicted_test_data = model.transform(test_data)


    # Evaluate the model performance
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol='label_indexed',
                                                     predictionCol='prediction', 
                                                     metricName='f1')
    print("F1 score: {}", evaluator_f1.evaluate(predicted_test_data))

    evaluator_accuracy = MulticlassClassificationEvaluator(labelCol='label_indexed',
                                                           predictionCol='prediction', 
                                                           metricName='accuracy')
    print("Accuracy: {}", evaluator_accuracy.evaluate(predicted_test_data)) 


    # Save the model
    model.save("hdfs://devenv/user/spark/spark_mllib_101/spam_detection/spam_classifier")


    # Read the saved model
    model = RandomForestClassificationModel.load("hdfs://devenv/user/spark/spark_mllib_101/spam_detection/spam_classifier") 


    # Predict some new records
    # In real case, use VectorAssembler to transform df for features column

    data = spark.read.csv("hdfs://devenv/user/spark/spark_mllib_101/spam_detection/data/sms_messages.csv",
                          inferSchema=True,
                          header=True)


    # Preprocessing and feature engineering
    feature_prep = data.select(lower(data["message"]).alias("message"), length(data["message"]).alias("length"))

    feature_prep = RegexTokenizer(inputCol="message", outputCol="words", pattern="\\W+").transform(feature_prep)

    feature_prep = StopWordsRemover(inputCol='words',outputCol='stop_words_removed').transform(feature_prep)

    feature_prep = HashingTF(inputCol="stop_words_removed", outputCol="hashing_tf", numFeatures=4000).transform(feature_prep)

    feature_prep = IDF(inputCol="hashing_tf", outputCol="tf_idf").fit(feature_prep).transform(feature_prep)

    unclassified_final_data = VectorAssembler(inputCols=["tf_idf", "length"],
                                              outputCol="features").transform(feature_prep)

    predicted_final_data = model.transform(unclassified_final_data)

    result = predicted_final_data.select("message", "prediction")

    result.show(1000)