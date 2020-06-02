import requests
import json

def get_games():

    url = "https://api.collegefootballdata.com/games?year=2019&seasonType=regular"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    games = json.loads(response.text)

    return games

data = get_games()

with open('../data/games.json', 'w') as fp:
    json.dump(data, fp, indent=4)