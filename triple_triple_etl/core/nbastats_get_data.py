import requests

HEADERS = {
    'user-agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/45.0.2454.101 Safari/537.36'),
    'referer': 'http://stats.nba.com/player/'
}


def get_data(base_url, params, headers=HEADERS):
    response = requests.get(url=base_url, params=params, headers=headers)

    if response.status_code != 200:
        response.raise_for_status()

    return response.json()
