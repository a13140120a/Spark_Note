from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create SparkSession
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Create a DataFrame from a CSV file with header
    stocks_df = spark.read.csv("hdfs://devenv/user/spark/spark_sql_101/data/stocks.csv",
                               header=True,
                               schema="symbol string, day date, open float, high float, low float, \
                                       close float, volume long, adj_close float")

    # Output result to console and write to hdfs in parquet file formats
    stocks_df.write.parquet("hdfs://devenv/user/spark/spark_sql_101/data/stock_in_parquet")
