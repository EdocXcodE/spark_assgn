from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CovidDataAnalysis") \
    .getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("covid_data.csv", header=True, inferSchema=True)

# Show the schema of the DataFrame
df.printSchema()

# Display the first few rows of the DataFrame
df.show()

# 2.1) Most affected country (total death/total covid cases)
most_affected_country = \
    df.withColumn("affected_ratio", df["deaths"] / df["cases"]).orderBy("affected_ratio", ascending=False).first()[
        "country"]

# 2.2) Least affected country (total death/total covid cases)
least_affected_country = \
    df.withColumn("affected_ratio", df["deaths"] / df["cases"]).orderBy("affected_ratio").first()["country"]

# 2.3) Country with highest covid cases
country_with_highest_cases = df.orderBy("cases", ascending=False).first()["country"]

# 2.4) Country with minimum covid cases
country_with_minimum_cases = df.orderBy("cases").first()["country"]

# 2.5) Total cases
total_cases = df.agg({"cases": "sum"}).collect()[0][0]

# 2.6) Country that handled the covid most efficiently (total recovery/total covid cases)
most_efficient_country = \
    df.withColumn("recovery_ratio", df["recovered"] / df["cases"]).orderBy("recovery_ratio", ascending=False).first()[
        "country"]

# 2.7) Country that handled the covid least efficiently (total recovery/total covid cases)
least_efficient_country = \
    df.withColumn("recovery_ratio", df["recovered"] / df["cases"]).orderBy("recovery_ratio").first()["country"]

# 2.8) Country least suffering from covid (least critical cases)
least_suffering_country = df.orderBy("critical").first()["country"]

# 2.9) Country still suffering from covid (highest critical cases)
still_suffering_country = df.orderBy("critical", ascending=False).first()["country"]

print("2.1) Most affected country (total death/total covid cases):", most_affected_country)
print("2.2) Least affected country (total death/total covid cases):", least_affected_country)
print("2.3) Country with highest covid cases:", country_with_highest_cases)
print("2.4) Country with minimum covid cases:", country_with_minimum_cases)
print("2.5) Total cases:", total_cases)
print("2.6) Country that handled the covid most efficiently (total recovery/total covid cases):",
      most_efficient_country)
print("2.7) Country that handled the covid least efficiently (total recovery/total covid cases):",
      least_efficient_country)
print("2.8) Country least suffering from covid (least critical cases):", least_suffering_country)
print("2.9) Country still suffering from covid (highest critical cases):", still_suffering_country)
