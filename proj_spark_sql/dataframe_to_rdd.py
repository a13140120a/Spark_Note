from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create SparkSession
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Create a DataFrame from a CSV file with header
    stocks = spark.read.csv("hdfs://devenv/user/spark/spark_sql_101/data/stocks.csv",
                            header=True,
                            schema="symbol string, day date, open float, high float, low float, \
                                    close float, volume long, adj_close float")

    # Get the rdd from the DataFrame
    stocks_rdd = stocks.rdd

    # Call RDD collect() method to trigger a job and collect the result back to the Driver
    stocks_rdd_list = stocks_rdd.collect()
