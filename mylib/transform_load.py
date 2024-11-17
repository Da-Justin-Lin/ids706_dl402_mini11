from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DELTA_PATH = "dbfs:/FileStore/mini_project11/iris_delta"

def transform_and_load():
    """
    Transform the Iris dataset and load it into a Delta table.
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Transform and Load Iris Dataset") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    # Read the Iris dataset from DBFS
    input_path = "dbfs:/FileStore/mini_project11/iris.csv"
    df = spark.read.csv(input_path, header=False, inferSchema=True)

    # Assign column names to the dataset
    columns = ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]
    df = df.toDF(*columns)

    # Show initial data
    print("Initial dataset:")
    df.show(5)

    # Transformation: Filter rows for species 'Iris-setosa'
    transformed_df = df.filter(col("species") == "Iris-setosa")

    # Show transformed data
    print("Transformed dataset:")
    transformed_df.show()

    # Write the transformed data to a Delta table
    print(f"Writing transformed data to Delta table at {DELTA_PATH}...")
    transformed_df.write.format("delta").mode("overwrite").save(DELTA_PATH)
    print(f"Data written successfully to {DELTA_PATH}.")

if __name__ == "__main__":
    transform_and_load()
