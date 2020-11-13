from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create SparkSession
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Create a DataFrame from a CSV file with header
    stocks_df = spark.read.csv("hdfs://devenv/user/spark/spark_sql_101/data/stocks.csv",
                               header=True,
                               inferSchema=True)

    # Show the schema of a DataFrame
    stocks_df.printSchema()

    # use SQL to query teh DataFrame with spark.sql() methods
    stocks_df.createOrReplaceTempView("stocks")
    result_df = spark.sql("""select symbol, avg(open) as avg_open, avg(close) as avg_close, count(1) as rec_count
                             FROM stocks
                             GROUP BY symbol""")

    # To cache the result in the first job
    result_df.persist()

    # Output result to console and write to hdfs in parquet file formats
    result_df.show()
    result_df.write.json("hdfs://devenv/user/spark/spark_sql_101/data/stocks_in_json2")
