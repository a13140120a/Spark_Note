from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Load full data
    logs = spark.read.parquet("hdfs://devenv/user/spark/spark_mllib_101/ec_web_logs_analysis/data/")

    # Hour info added
    logs = logs.withColumn("hour", substring("timestamp", 11, 3))

    # Stats of hourly activities
    device_hourly_activity_stats = logs.select("device_id", "hour").groupBy("hour") \
        .agg(count("*").alias("visit_count"), countDistinct("device_id").alias("uu_count")).orderBy("hour")

    device_hourly_activity_stats.write.option("driver", "com.mysql.jdbc.Driver")\
        .jdbc("jdbc:mysql://192.168.186.139:3306", "ec_web_logs_analysis.stats_device_hourly_activities",
              properties={"user": "spark", "password": "spark"}, mode="overwrite")
