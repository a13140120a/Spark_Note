from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create SparkSession
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Create a DataFrame from a folder containing parquet files
    stocks_df = spark.read.parquet("hdfs://devenv/user/spark/spark_sql_101/data/stocks_stats_parquet1")

    # Show the schema of a DataFrame
    stocks_df.printSchema()

    # use SQL to query teh DataFrame with spark.sql() methods
    stocks_df.createOrReplaceTempView("stocks_stats")
    result_df = spark.sql("select * from stocks_stats")

    # Output result to the console
    result_df.show()
