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

    # Gender prediction
    # Load gender model
    gender_model = RandomForestClassificationModel.load(
        "hdfs://devenv/user/spark/spark_mllib_101/ec_web_logs_analysis/model_gender_prediction/")
    # +------+----------+
    # |gender|prediction|
    # +------+----------+
    # |     1|       1.0|
    # |     0|       0.0|
    # +------+----------+

    # Prepare features and preprocessing
    data_prep = logs.select("device_id", "product_category_id", "device_type", "connect_type", "gender")

    data_prep = VectorAssembler(inputCols=["product_category_id", "device_type", "connect_type"],
                                outputCol="features").transform(data_prep)

    # Predict gender
    predicted_result = gender_model.transform(data_prep)

    # Tagging inferred gender for each device and output to MySQL
    device_predictions = predicted_result.select("device_id", "prediction")

    temp1 = device_predictions.groupBy("device_id", "prediction").count()
    temp2 = temp1.select("device_id", "count").groupBy("device_id").agg(max("count").alias("count"))

    device_inferred_gender = temp1.join(temp2, ["device_id", "count"])\
        .drop("count")\
        .withColumnRenamed("prediction", "inferred_gender")

    device_inferred_gender.write.option("driver", "com.mysql.jdbc.Driver") \
        .jdbc("jdbc:mysql://192.168.186.139:3306", "ec_web_logs_analysis.device_inferred_gender",
              properties={"user": "spark", "password": "spark"}, mode="overwrite")

    # Summarize stats
    device_inferred_gender.groupBy("inferred_gender")\
        .count() \
        .write.option("driver", "com.mysql.jdbc.Driver")\
        .jdbc("jdbc:mysql://192.168.186.139:3306", "ec_web_logs_analysis.stats_inferred_gender_count",
              properties={"user": "spark", "password": "spark"}, mode="overwrite")
