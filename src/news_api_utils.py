import os
import json
import requests
import pandas as pd
# from dotenv import load_dotenv

def get_news_search(key, start_date=None, end_date=None, keyword=None, region=None, size=200, page=None):
    """
    Retrieves news based on search parameters.
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

    if region:
        params['country'] = region

    if page:
        params['page'] = page

    response = requests.get(url, params=params)
    data = response.json()

    return data


def fetch_news_to_csv(api_key, start, end, keyword, output_csv):
    """
    Fetches news for a date range and saves directly to a CSV file inside data/ folder.
    """

    # Ensure data folder exists
    os.makedirs("data", exist_ok=True)

    final_df = pd.DataFrame()

    # Iterate through date range
    dates = pd.date_range(start=start, end=end)

    for d in dates:
        start_date = d.strftime("%Y-%m-%dT00:00:00Z")
        end_date   = d.strftime("%Y-%m-%dT23:59:59Z")

        print(f"Fetching news on {d.strftime('%Y-%m-%d')}...")

        data = get_news_search(api_key, start_date=start_date, end_date=end_date, keyword=keyword)

        if data['status'] != "ok":
            print(f"Error: {data.get('msg', 'Unknown error')}")
            continue

        news_list = data['news']

        # Convert directly into DataFrame
        daily_df = pd.DataFrame(news_list)
        final_df = pd.concat([final_df, daily_df], ignore_index=True)

    # Save to CSV in data/ directory
    output_path = os.path.join("data", output_csv)
    final_df.to_csv(output_path, index=False)

    print(f"\n✅ CSV saved successfully to: {output_path}")


if __name__ == "__main__":

    API_KEY = input("Please enter your API key: ").strip()
    start = input("Please enter your start date in YYYY-MM-DD format: ").strip()
    end = input("Please enter your end date in YYYY-MM-DD format: ").strip()
    output_name = input("Please enter your output csv file name (must end in .csv): ").strip()
    keyword = input("Please enter your keyword: ").strip()

    # Call the main function with user-provided arguments
    fetch_news_to_csv(
        api_key = API_KEY,
        start = start,
        end = end,
        keyword = keyword,
        output_csv = output_name
    )

    print(f"✔ Download completed. CSV saved as: {output_name}")
