from pyspark.sql import SparkSession
from pyspark.sql.functions import year

# Create SparkSession
spark = SparkSession.builder.appName("Data frames").getOrCreate()

###################
# Data Loading
###################

# Read the CSV file into a DataFrame
df = spark.read\
    .options(inferSchema='True', header='True')\
    .csv('Datasets.csv')

# df.show()

# Extract the year from the timestamp column
df_with_year = df.withColumn("year", year(df["Issue Date"]))

print(df_with_year)

# # Calculate the total number of tickets for each year
# total_tickets_by_year = df_with_year.groupBy("year").count()
#
# # Show the result
# total_tickets_by_year.show()
