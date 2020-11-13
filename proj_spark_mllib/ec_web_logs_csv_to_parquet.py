from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Prepare data
    raw = spark.read.csv("hdfs://devenv/user/spark/spark_mllib_101/ec_web_logs_analysis/raw/", 
                        header=True,
                        schema="device_id string,timestamp timestamp,product_category_id int,ip string,lat float, lon float,\
                              device_type int,connect_type int,age_group string,gender int,member_id string")

    raw.write.parquet("hdfs://devenv/user/spark/spark_mllib_101/ec_web_logs_analysis/data/")