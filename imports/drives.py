import requests
import json

def get_drives():
    url = "https://api.collegefootballdata.com/drives?year=2019&seasonType=regular"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    drives = json.loads(response.text)

    return drives

data = get_drives()

with open('../data/drives.json', 'w') as fp:
    json.dump(data, fp, indent=4)