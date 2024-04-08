import requests
import pandas as pd
from pyspark.sql import SparkSession

# Make a request to the API
url = ("https://disease.sh/v3/covid-19/countries/india,france,germany,brazil,south%20korea,japan,italy,uk,russia,"
       "turkey,spain,australia,vietnam,taiwan,argentina,netherlands,mexico,iran,indonesia")
response = requests.get(url)
data = response.json()

# Extract required columns
columns = ["country","cases", "todayCases", "deaths", "todayDeaths", "recovered", "todayRecovered",
           "active", "critical", "casesPerOneMillion", "deathsPerOneMillion", "tests",
           "testsPerOneMillion", "population", "continent", "oneCasePerPeople", "oneDeathPerPeople",
           "oneTestPerPeople", "activePerOneMillion", "recoveredPerOneMillion", "criticalPerOneMillion"]

# Create a DataFrame
df = pd.DataFrame(data)
df = df[columns]

# Write DataFrame to CSV file
df.to_csv("covid_data.csv", index=False)

# Create a SparkSession
spark = SparkSession.builder \
    .appName("COVID Data Analysis") \
    .getOrCreate()

# Create a Spark DataFrame from CSV file
spark_df = spark.read.csv("covid_data.csv", header=True, inferSchema=True)

# Show the Spark DataFrame
spark_df.show()






