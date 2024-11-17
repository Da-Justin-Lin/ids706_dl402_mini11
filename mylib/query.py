from pyspark.sql import SparkSession

def query_transform_iris():
    """
    Run a predefined SQL query on the Iris dataset stored as a Delta table.
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Query Iris Dataset") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    # Define the SQL query to filter Iris dataset using Delta table path
    delta_path = "dbfs:/FileStore/mini_project11/iris_delta"
    query = (
        f"SELECT * FROM delta.`{delta_path}` WHERE species = 'Iris-setosa'"
    )

    # Execute the query
    query_result = spark.sql(query)

    # Show the query result
    print("Query Result:")
    query_result.show()

    # Return the result as a DataFrame
    return query_result


if __name__ == "__main__":
    query_transform_iris()
