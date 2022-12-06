import requests

def request(url):
    client = requests.Session()
    # client.params.update({'app_id': API_KEY})

    response = client.get(url)
    return response.json()

