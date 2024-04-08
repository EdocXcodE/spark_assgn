from pyspark.sql import SparkSession
from flask import Flask, jsonify

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CovidDataAnalysis") \
    .getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("covid_data.csv", header=True, inferSchema=True)

# Initialize Flask
app = Flask(__name__)


# 2.1) Most affected country (total death/total covid cases)
@app.route('/most_affected_country', methods=['GET'])
def most_affected_country_api():
    result = \
        df.withColumn("affected_ratio", df["deaths"] / df["cases"]).orderBy("affected_ratio", ascending=False).first()[
            "country"]
    return jsonify({"most_affected_country": result})


# 2.2) Least affected country (total death/total covid cases)
@app.route('/least_affected_country', methods=['GET'])
def least_affected_country_api():
    result = df.withColumn("affected_ratio", df["deaths"] / df["cases"]).orderBy("affected_ratio").first()["country"]
    return jsonify({"least_affected_country": result})


# 2.3) Country with highest covid cases
@app.route('/country_with_highest_cases', methods=['GET'])
def country_with_highest_cases_api():
    result = df.orderBy("cases", ascending=False).first()["country"]
    return jsonify({"country_with_highest_cases": result})


# 2.4) Country with minimum covid cases
@app.route('/country_with_minimum_cases', methods=['GET'])
def country_with_minimum_cases_api():
    result = df.orderBy("cases").first()["country"]
    return jsonify({"country_with_minimum_cases": result})


# 2.5) Total cases
@app.route('/total_cases', methods=['GET'])
def total_cases_api():
    result = df.agg({"cases": "sum"}).collect()[0][0]
    return jsonify({"total_cases": result})


# 2.6) Country that handled the covid most efficiently (total recovery/total covid cases)
@app.route('/most_efficient_country', methods=['GET'])
def most_efficient_country_api():
    result = \
        df.withColumn("recovery_ratio", df["recovered"] / df["cases"]).orderBy("recovery_ratio",
                                                                               ascending=False).first()[
            "country"]
    return jsonify({"most_efficient_country": result})


# 2.7) Country that handled the covid least efficiently (total recovery/total covid cases)
@app.route('/least_efficient_country', methods=['GET'])
def least_efficient_country_api():
    result = df.withColumn("recovery_ratio", df["recovered"] / df["cases"]).orderBy("recovery_ratio").first()["country"]
    return jsonify({"least_efficient_country": result})


# 2.8) Country least suffering from covid (least critical cases)
@app.route('/least_suffering_country', methods=['GET'])
def least_suffering_country_api():
    result = df.orderBy("critical").first()["country"]
    return jsonify({"least_suffering_country": result})


# 2.9) Country still suffering from covid (highest critical cases)
@app.route('/still_suffering_country', methods=['GET'])
def still_suffering_country_api():
    result = df.orderBy("critical", ascending=False).first()["country"]
    return jsonify({"still_suffering_country": result})


if __name__ == '__main__':
    # Run the Flask app
    app.run(debug=True)
