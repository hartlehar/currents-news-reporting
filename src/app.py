import os, json, requests
from dotenv import load_dotenv
import pandas as pd
import glob

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


def save_to_csv(path, filename):
    """
    Saves news data from JSON files in the specified path to a CSV file.
    Parameters:
        path (str): Path to the directory containing JSON file(s) of news data.
        filename (str): Name of the output CSV file.
    """
    files = glob.glob(f"{path}/*.json")

    news_dict = {
        'id': [],
        'title': [],
        'description': [],
        'author': [],
        'url': [],
        'image': [],
        'language': [],
        'category': [],
        'published': []
    }

    for file in files:
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if data['status'] == 'ok':    
                news = data['news']

                for n in news:
                    for k in news_dict.keys():
                        news_dict[k].append(n[k])

    dfs = pd.DataFrame(news_dict)
    dfs.to_csv(filename, index=False)



if __name__ == "__main__":
    # load API key from .env file
    load_dotenv()
    API_KEY = os.getenv('API_KEY')
    
    # get news from date range
    dates = pd.date_range(start="2025-09-10", end="2025-09-11")
    
    for d in dates:
        date = d.strftime("%Y-%m-%dT00:00:00Z")

        data = get_news_search(API_KEY, start_date=date, end_date=date, keyword='world')

        # print message and skip if not valid response
        if data['status'] != "ok":
            print(data['msg'])

        # save to json file
        with open(f'data_{d.strftime("%Y_%m_%d")}.json', 'w') as f:
            json.dump(data, f)    


