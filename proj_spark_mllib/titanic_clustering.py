from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler,VectorIndexer,OneHotEncoder,StringIndexer
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Prepare data
    raw = spark.read.csv("hdfs://devenv/user/spark/spark_mllib_101/titanic",
                          inferSchema=True,
                          header=True)

    # Preprocessing and feature engineering
    data = raw.select("Survived","Pclass","Sex","Age","Fare","Embarked") \
              .dropna()

    feature_prep = StringIndexer(inputCol="Sex",outputCol="SexIndex").fit(data).transform(data)

    feature_prep = OneHotEncoder(inputCol="SexIndex",outputCol="SexVec").transform(feature_prep)

    feature_prep = StringIndexer(inputCol="Embarked",outputCol="EmbarkIndex").fit(feature_prep).transform(feature_prep)

    feature_prep = OneHotEncoder(inputCol="EmbarkIndex",outputCol="EmbarkVec").transform(feature_prep)

    final_data = VectorAssembler(inputCols=["Survived","Pclass","SexVec","Age","Fare","EmbarkVec"],
                                 outputCol="features").transform(feature_prep)

    # Split data into train and test sets
    # Nor necessary for Clustering
    
    # Model training
    kmeans = KMeans(k=5)
    model = kmeans.fit(final_data)
    
    # Transform the test data using the model to get predictions
    clustered_data = model.transform(final_data)

    # Prediction and model status
    clustered_data_sorted = clustered_data.orderBy("prediction")
    clustered_data_sorted.show(10000) #show all

    clustered_data.groupBy("prediction").agg(avg("Survived"),
                                             avg("Pclass"),
                                             avg("Age"),
                                             avg("Fare"),
                                             avg("SexIndex"),
                                             avg("EmbarkIndex"),
                                             count("prediction")) \
                                        .orderBy("prediction").show()
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)


    # Evaluate the model performance
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(clustered_data)
    print("Silhouette:", silhouette)
