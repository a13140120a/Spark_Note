from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Create a list of tuple
    people = [(1, "John", 25), (2, "Peter", 15), (3, "Mary", 20)]

    # Prepare schema
    schema = "id: int, name: string, age: int"

    # Create DataFrame
    people_df = spark.createDataFrame(people, schema)

    # Show the schema of a DataFrame and result
    people_df.printSchema()

    # use SQL to query teh DataFrame with spark.sql() methods
    people_df.createOrReplaceTempView("people")
    over_18 = spark.sql("""select * from people where age >= 18 """)

    # Output result to console and write to hdfs in parquet file formats
    over_18.show()