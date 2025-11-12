import os, json, requests
from dotenv import load_dotenv
import pandas as pd

def get_latest_news(key):
    """
    Retrieves the latest news.
    Parameters:
        key (str): API key for authentication.
    Returns:
        dict: JSON response containing the latest news articles.
    """
    url = "https://api.currentsapi.services/v1/latest-news"

    params = {
        'apiKey': key
    }

    response = requests.get(url, params=params)
    data = response.json()

    return data


def get_news_search(key, start_date=None, end_date=None, keyword=None, region=None, size=200, page=None):
    """
    Retrieves news based on search parameters.
    Parameters:
        key (str): API key for authentication.
        start_date (str): Start date for the news search in 'YYYY-MM-DDTHH:MM:SS.ss[+-]hh:mm' format.
        end_date (str): End date for the news search in 'YYYY-MM-DDTHH:MM:SS.ss[+-]hh:mm' format.
        keyword (str): Keyword to search for in title or description of news articles.
        region (str): Region code to filter news articles.
        size (int): Number of articles to retrieve per page (default is 200).
        page (int): Page number for pagination.
    Returns:
        dict: JSON response containing the news articles.
    """
    url = 'https://api.currentsapi.services/v1/search'

    params = {
        'apiKey': key,
        'page_size': size
    }

    if start_date:
        params['start_date'] = start_date
    
    if end_date:
        params['end_date'] = end_date

    if keyword:
        params['keyword'] = keyword

    if page:
        params['page'] = page

    if region:
        params['country'] = region

    response = requests.get(url, params=params)
    data = response.json()

    return data


def get_categories(key):
    """
    Retrieves available news categories.
    Parameters:
        key (str): API key for authentication.
    Returns:
        dict: JSON response containing the available news categories.
    """
    url = 'https://api.currentsapi.services/v1/available/categories'

    params = {
        'apiKey': key
    }

    response = requests.get(url, params=params)
    data = response.json()
    
    return data


def get_regions(key):
    """
    Retrieves valid region codes.
    Parameters:
        key (str): API key for authentication.
    Returns:
        dict: JSON response containing the region codes.
    """
    url = 'https://api.currentsapi.services/v1/available/regions'

    params = {
        'apiKey': key
    }

    response = requests.get(url, params=params)
    data = response.json()
    
    return data


if __name__ == "__main__":
    # load API key from .env file
    load_dotenv()
    API_KEY = os.getenv('API_KEY')
    
    # get news from date range
    dates = pd.date_range(start="2025-08-22", end="2025-08-22")

    for d in dates:
        date = d.strftime("%Y-%m-%dT00:00:00Z")

        data = get_news_search(API_KEY, region='CN')

        # print message and skip if not valid response
        if data['status'] != "ok":
            print(data['msg'])

        # save to json file
        with open(f'china_data.json', 'w') as f:
            json.dump(data, f)


