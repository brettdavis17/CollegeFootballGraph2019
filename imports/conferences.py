import requests
import json

def get_conferences():

    url = "https://api.collegefootballdata.com/conferences"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    conferences = json.loads(response.text)

    return conferences

data = get_conferences()

with open('../data/conferences.json', 'w') as fp:
    json.dump(data, fp, indent=4)