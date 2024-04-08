# COVID-19 Countrywise Data Analysis

This project fetches COVID-19 data for selected countries from the [disease.sh API](https://disease.sh/docs/#/COVID-19%3A%20Worldometers/get_v3_covid_19_countries) and performs data analysis using PySpark DataFrame. Additionally, it exposes an API using Flask to display the analyzed data.

## Overview

The project consists of the following main components:

1. **Data Fetching**: Utilizes the disease.sh API to fetch COVID-19 data for 20 countries.

2. **Data Processing**: Processes the fetched data using PySpark DataFrame to perform required operations such as filtering, aggregation, and transformation.

3. **API Development**: Develops a Flask application to create a development server and expose APIs to display the analyzed data.

4. **Data Visualization**: Presents the analyzed data through API endpoints in JSON format for visualization or further consumption.

## How to Use

To run the project locally, follow these steps:

1. Clone the repository:

   ```bash
   git clone <repository-url>

2. Navigate to the project directory:

   cd <project-directory>

3. To run flask app:
   
   python <file_name>.py

