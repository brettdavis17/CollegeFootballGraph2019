import requests
import json
import pandas as pd

weeks = list(range(1, 16))

def get_plays(week):

    url = "https://api.collegefootballdata.com/plays?seasonType=regular&year=2019&week=" + str(week)

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    plays = json.loads(response.text)

    return plays

for week in weeks:

    data = get_plays(week)

    for d in data:

        if 'clock' in d:

            d['clockSec'] = d['clock']['minutes'] * 60 + d['clock']['seconds'] if 'minutes' in d['clock'] \
                else d['clock']['seconds']

            d['gameTimeLeftSec'] = d['clockSec'] + 2700 if d['period'] == 1 \
                else d['clockSec'] + 1800 if d['period'] == 2 \
                    else d['clockSec'] + 900 if d['period'] == 3 \
                        else d['clockSec']

            if 'minutes' in d['clock']:
                d['clockDisplay'] = str(int(d['clockSec'] / 60)) + ':' + str(int(d['clockSec'] % 60)) \
                    if int(d['clockSec'] % 60) >= 10 \
                        else str(int(d['clockSec'] / 60)) + ':0' + str(int(d['clockSec'] % 60))
            else:
                d['clockDisplay'] = '00:' + str(int(d['clockSec'] % 60)) \
                    if int(d['clockSec'] % 60) >= 10 \
                        else '00:0' + str(int(d['clockSec'] % 60))

    df_data = []

    df_columns = []

    for d in data:

        df_row_data = [v for k, v in d.items()]

        df_data.append(df_row_data)

        df_columns = [k for k, v in d.items()]

    unsorted_df = pd.DataFrame(
        df_data,
        columns=df_columns
    )

    sorted_df = unsorted_df.sort_values([
        'drive_id',
        'gameTimeLeftSec'
    ], ascending=[
        1,
        0
    ]
    ).reset_index(drop=True)

    sorted_df['index'] = sorted_df.index

    sorted_data = json.loads(
        sorted_df.to_json(
            orient='records'
    )
    )

    with open('../data/plays/week' + str(week) + '.json', 'w') as fp:
        json.dump(sorted_data, fp, indent=4)


