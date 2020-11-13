from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .getOrCreate()
    
    # (1) Define a normal Python function and match arguments to your UDF (i.e. number of arguments and their types)
    def slen_py(s):
        return len(s)

    # (2) Register UDF function
    spark.udf.register("slen", slen_py, IntegerType())
    slen = udf(slen_py, IntegerType())

    # Use it
    df = spark.read.csv("hdfs://devenv/user/spark/spark_sql_101/data/stocks.csv",
                        header=True,
                        inferSchema=True)

    df.printSchema()
    df.show()

    df.createOrReplaceTempView("stocks")
    spark.sql("select slen(symbol) as length_of_symbol from stocks").show()

    df.select(slen("symbol").alias("length_of_symbol")).show()
