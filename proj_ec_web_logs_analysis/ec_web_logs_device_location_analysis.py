from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Prepare data
    logs = spark.read.parquet("hdfs://devenv/user/spark/spark_mllib_101/ec_web_logs_analysis/data/")

    # Tagging places the users go frequently for each device and output to MySQL
    all_device_ids = logs.select("device_id") \
        .distinct() \
        .rdd.map(lambda row: row[0]).collect()


    len_all_device_ids = len(all_device_ids)

    for i in range(len_all_device_ids):
        print("{}/{} processed.".format(i,len_all_device_ids))

        device_locations = logs.select("device_id", "lat", "lon")\
            .where("device_id = '{}'".format(all_device_ids[i]))

        device_locations = VectorAssembler(inputCols=["lat", "lon"],
                                           outputCol="features").transform(device_locations)
        # Model training
        kmeans = KMeans(k=10)
        model = kmeans.fit(device_locations)

        # Transform the test data using the model to get predictions
        predicted_device_locations = model.transform(device_locations)

        # Cluster centers and count
        device_inferred_location = predicted_device_locations.groupBy("device_id", "prediction") \
            .agg(avg("lat").alias("avg_lat"), avg("lon").alias("avg_lon"), count("*").alias("lat_lon_count")) \
            .drop("prediction")

        device_inferred_location.persist()

        device_inferred_location.show()

        device_inferred_location.write.option("driver", "com.mysql.jdbc.Driver") \
            .jdbc("jdbc:mysql://192.168.186.139:3306", "ec_web_logs_analysis.device_inferred_location",
                  properties={"user": "spark", "password": "spark"}, mode="append")
