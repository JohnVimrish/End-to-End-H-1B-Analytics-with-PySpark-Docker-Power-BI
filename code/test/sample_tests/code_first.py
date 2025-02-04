from pyspark.sql import SparkSession

# Step 2: Initialize SparkSession with Configurations
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Step 3: Load Some Sample Data
data = [
    ("John", 28, "California"),
    ("Doe", 22, "New York"),
    ("Alice", 30, "Texas"),
    ("Bob", 35, "Florida")
]
columns = ["Name", "Age", "State"]
df = spark.createDataFrame(data, columns)

# Step 4: Perform Some Transformations and Actions
print("Original DataFrame:")
df.show()

# Filter rows where Age > 25
df_filtered = df.filter(df.Age > 25)
print("Filtered DataFrame (Age > 25):")
df_filtered.show()

# Group by State and calculate the average age
df_grouped = df.groupBy("State").avg("Age")
print("Grouped DataFrame with Average Age by State:")
df_grouped.show()

# Step 5: Stop the Spark Session
spark.stop()
