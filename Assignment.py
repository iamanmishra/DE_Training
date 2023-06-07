from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, udf, regexp_replace, year, countDistinct, col, count, desc, when
from pyspark.sql.types import DateType

# Create SparkSession
spark = SparkSession.builder.appName("Data frames").getOrCreate()

####################
#   Data Loading   #
####################

# Read the CSV file into a DataFrame
df = spark.read \
    .options(inferSchema='True', header='True') \
    .csv('Datasets.csv')

print("Total number of rows = {}".format(df.count()))

# drop all empty row
df_filter = df.na.drop("all")
print("Total number of rows after removing empty rows = {}".format(df_filter.count()))


####################
# Examine the data #
####################

# converting all date into single format
date_df = df_filter.withColumn('Issue Date', regexp_replace('Issue Date', '/', '-'))
date_df.select('Issue Date').show(10)

date_df = date_df.withColumn(
    'Issue Date',
    F.to_date(
        F.unix_timestamp('Issue Date', 'MM-dd-yyyy').cast('timestamp')))

date_df.select('Issue Date').show(10)

df_with_year = date_df.withColumn("year", year(date_df["Issue Date"]))

total_tickets_by_year = df_with_year.groupBy("year").count().orderBy(F.asc("year"))

total_tickets_by_year.show()

unique_states_count = date_df.select("Registration State").distinct().count()

print("Number of unique states:", unique_states_count)

# Step 1: Find the state with the maximum entries
state_entries = date_df.groupBy("Registration State").agg(count("*").alias("count")).orderBy(desc("count"))

state_entries.show()

state_with_max_entries = state_entries.select("Registration State").first()[0]

print("State with max entries is", state_with_max_entries)

# Step 2: Replace '99' entries with the state having maximum entries
df_corrected = date_df.withColumn("Registration State", when(col("Registration State") == '99', state_with_max_entries).
                                  otherwise(col("Registration State")))

# Step 3: Count the number of unique states again
unique_states_count = df_corrected.select("Registration State").distinct().count()

print("Number of unique states after correction:", unique_states_count)



#####################
# Aggregation tasks #
#####################

# Filter out rows with null violation code
df_violation_filtered = df.filter(col("Violation Code").isNotNull())

# Group by violation code and count the occurrences
violation_counts = df_violation_filtered.groupBy("Violation Code").count()

# Sort the counts in descending order
sorted_counts = violation_counts.orderBy(desc("count"))

# Display the top five violation codes
top_five = sorted_counts.limit(5)

# Show the results
top_five.show()

# Filter out rows with null Vehicle Body Type
df_body_type_filtered = df.filter(col("Violation Code").isNotNull())

# Group by 'vehicle body type' and count the occurrences
body_type_counts = df_body_type_filtered.groupBy("Vehicle Body Type").count()

# Sort the counts in descending order
sorted_body_type_counts = body_type_counts.orderBy(col("count").desc())

# Display the top five 'vehicle body types'
top_five_body_types = sorted_body_type_counts.limit(5)

# Show the results for 'vehicle body types'
print("Top 5 Vehicle Body Types:")
top_five_body_types.show()

df_vehicle_make_filtered = df.filter(col("Vehicle Make").isNotNull())

# Group by 'vehicle make' and count the occurrences
make_counts = df_vehicle_make_filtered.groupBy("Vehicle Make").count()

# Sort the counts in descending order
sorted_make_counts = make_counts.orderBy(col("count").desc())

# Display the top five 'vehicle makes'
top_five_makes = sorted_make_counts.limit(5)

# Show the results for 'vehicle makes'
print("Top 5 Vehicle Makes:")
top_five_makes.show()

# Filter out erroneous entries with 'Violation Precinct' as '0' and group by 'Violation Precinct'
violation_precinct_counts = df.filter(col("Violation Precinct") != '0').groupBy("Violation Precinct").count()

# Sort the counts in descending order
sorted_violation_precinct_counts = violation_precinct_counts.orderBy(col("count").desc())

# Display the top five 'Violation Precincts'
top_five_violation_precincts = sorted_violation_precinct_counts.limit(5)

# Show the results for 'Violation Precincts'
print("Top 5 Violation Precincts:")
top_five_violation_precincts.show()

# Filter out erroneous entries with 'Issuer Precinct' as '0' and group by 'Issuer Precinct'
issuer_precinct_counts = df.filter(col("Issuer Precinct") != '0').groupBy("Issuer Precinct").count()

# Sort the counts in descending order
sorted_issuer_precinct_counts = issuer_precinct_counts.orderBy(col("count").desc())

# Display the top six 'Issuer Precincts'
top_six_issuer_precincts = sorted_issuer_precinct_counts.limit(6)

# Show the results for 'Issuer Precincts'
print("Top 6 Issuer Precincts:")
top_six_issuer_precincts.show()
