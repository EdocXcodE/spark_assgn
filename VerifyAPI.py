import requests


def verifyAPIQ3(route):
    url = f"http://127.0.0.1:5000/{route}"
    response = requests.get(url)
    data = response.json()
    return data


def verifyAPIQ4(route):
    url = f"http://127.0.0.1:5001/{route}"
    response = requests.get(url)
    data = response.json()
    return data


# Call the verifyAPI function for each endpoint and print the data
endpoints = [
    "most_affected_country",
    "least_affected_country",
    "country_with_highest_cases",
    "country_with_minimum_cases",
    "total_cases",
    "most_efficient_country",
    "least_efficient_country",
    "least_suffering_country",
    "still_suffering_country"
]

for endpoint in endpoints:
    print(f"Verifying endpoint: {endpoint}")
    data = verifyAPIQ3(endpoint)
    print(f"{endpoint} - {data[endpoint]}")
    print("-" * 50)


print("Verifying endpoint: covid-data\n")
print(verifyAPIQ4("covid-data"))
