import requests
import json

def get_teams():
    url = "https://api.collegefootballdata.com/teams"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    teams = json.loads(response.text)

    return teams

data = get_teams()

with open('../data/teams.json', 'w') as fp:
    json.dump(data, fp, indent=4)