import requests
import pandas as pd
from flask import Flask, jsonify

app = Flask(__name__)

# Read the COVID-19 data from the CSV file
df = pd.read_csv("covid_data.csv")


# Route to expose the data
@app.route('/covid-data', methods=['GET'])
def covid_data():
    data = df.to_dict(orient='records')
    return jsonify(data)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5001, debug=True)
