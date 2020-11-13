from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, VectorIndexer, OneHotEncoder, StringIndexer
from pyspark.ml.classification import RandomForestClassificationModel

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Load full data
    logs = spark.read.parquet("hdfs://devenv/user/spark/spark_mllib_101/ec_web_logs_analysis/data/")

    # Age group prediction
    # Load age group model
    age_group_model = RandomForestClassificationModel.load(
        "hdfs://devenv/user/spark/spark_mllib_101/ec_web_logs_analysis/model_age_group_prediction/")
    # +---------+-----------------+
    # |age_group|age_group_indexed|
    # +---------+-----------------+
    # | under 20|              2.0|
    # |  over 50|              3.0|
    # |    21-35|              0.0|
    # |    36-50|              1.0|
    # +---------+-----------------+

    # Prepare features and preprocessing
    data_prep = logs.select("device_id", "product_category_id", "device_type", "connect_type", "age_group")

    data_prep = VectorAssembler(inputCols=["product_category_id", "device_type", "connect_type"],
                                outputCol="features").transform(data_prep)

    # Predict gender
    predicted_result = age_group_model.transform(data_prep)

    # Tagging inferred gender for each device and output to MySQL
    device_predictions = predicted_result.select("device_id", "prediction")

    temp1 = device_predictions.groupBy("device_id", "prediction").count()
    temp2 = temp1.select("device_id", "count").groupBy("device_id").agg(max("count").alias("count"))

    device_inferred_age_group = temp1.join(temp2, ["device_id", "count"])\
        .drop("count")\
        .withColumnRenamed("prediction", "inferred_age_group")

    device_inferred_age_group.write.option("driver", "com.mysql.jdbc.Driver") \
        .jdbc("jdbc:mysql://192.168.186.139:3306", "ec_web_logs_analysis.device_inferred_age_group",
              properties={"user": "spark", "password": "spark"}, mode="overwrite")

    # Summarize stats
    device_inferred_age_group.groupBy("inferred_age_group")\
        .count() \
        .write.option("driver", "com.mysql.jdbc.Driver")\
        .jdbc("jdbc:mysql://192.168.186.139:3306", "ec_web_logs_analysis.stats_inferred_age_group_count",
              properties={"user": "spark", "password": "spark"}, mode="overwrite")
