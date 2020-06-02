import requests
import json

def get_venues():
    url = "https://api.collegefootballdata.com/venues"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    venues = json.loads(response.text)

    return venues

data = get_venues()

with open('../data/venues.json', 'w') as fp:
    json.dump(data, fp, indent=4)