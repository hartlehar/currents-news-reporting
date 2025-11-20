import os
import json
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv


def get_news_search(key, start_date=None, end_date=None, keyword=None, region=None, size=200, page=None):
    """
    Retrieves news based on search parameters from Currents API.
    
    Args:
        key: API key for authentication
        start_date: Start date in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        end_date: End date in ISO format
        keyword: Search keyword
        region: Country code
        size: Number of results per page (max 200)
        page: Page number for pagination
        
    Returns:
        dict: API response containing news articles
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

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()  # Raise exception for HTTP errors
        data = response.json()
        
        # Validate response structure
        if 'status' not in data:
            raise ValueError("Invalid API response: missing 'status' field")
            
        return data
        
    except requests.exceptions.Timeout:
        raise Exception("❌ API request timed out after 30 seconds")
    except requests.exceptions.ConnectionError:
        raise Exception("❌ Failed to connect to Currents API")
    except requests.exceptions.HTTPError as e:
        raise Exception(f"❌ HTTP error: {response.status_code} - {str(e)}")
    except Exception as e:
        raise Exception(f"❌ Unexpected error in API request: {str(e)}")


def fetch_news_to_csv(api_key, start, end, keyword, output_csv):
    """
    Fetches news for a date range and saves directly to a CSV file.
    
    Args:
        api_key: Currents API key
        start: Start date (YYYY-MM-DD)
        end: End date (YYYY-MM-DD)
        keyword: Search keyword
        output_csv: Output CSV filename
        
    Raises:
        ValueError: If no data is fetched or API errors occur
    """

    # Use absolute path /opt/airflow/data for consistency across containers
    data_dir = Path("./data")
    
    # Create directory if it doesn't exist
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 Data directory: {data_dir}")
    except Exception as e:
        raise Exception(f"❌ Failed to create data directory {data_dir}: {str(e)}")

    final_df = pd.DataFrame()
    total_articles = 0
    error_count = 0
    no_data_count = 0

    # Iterate through date range
    dates = pd.date_range(start=start, end=end)
    print(f"📅 Fetching news from {start} to {end}")
    print(f"🔑 Keyword: {keyword}")
    print(f"📊 Number of days: {len(dates)}")

    for d in dates:
        start_date = d.strftime("%Y-%m-%dT00:00:00Z")
        end_date = d.strftime("%Y-%m-%dT23:59:59Z")

        print(f"\n🌐 Fetching news on {d.strftime('%Y-%m-%d')}...")

        try:
            data = get_news_search(api_key, start_date=start_date, end_date=end_date, keyword=keyword)
        except Exception as e:
            print(f"❌ API request failed: {str(e)}")
            error_count += 1
            continue

        # Check API response status
        if data.get('status') != "ok":
            error_msg = data.get('msg', 'Unknown error')
            print(f"⚠️ API error on {d.strftime('%Y-%m-%d')}: {error_msg}")
            error_count += 1
            continue

        # Get news articles from response
        news_list = data.get('news', [])
        
        if not news_list:
            print(f"ℹ️ No news found for {d.strftime('%Y-%m-%d')}")
            no_data_count += 1
            continue
            
        # Convert to DataFrame and append
        try:
            daily_df = pd.DataFrame(news_list)
            articles_today = len(daily_df)
            total_articles += articles_today
            final_df = pd.concat([final_df, daily_df], ignore_index=True)
            print(f"✅ Found {articles_today} articles")
        except Exception as e:
            print(f"❌ Error processing data for {d.strftime('%Y-%m-%d')}: {str(e)}")
            error_count += 1
            continue

    # Validate we got some data
    if final_df.empty:
        error_summary = f"Errors: {error_count} days, No data: {no_data_count} days"
        raise ValueError(
            f"❌ No news data fetched! ({error_summary})\n"
            "Please check:\n"
            "  - API key is valid\n"
            "  - Date range is correct\n"
            "  - Keyword is valid\n"
            "  - Not exceeding API rate limits"
        )

    # Save to CSV
    try:
        output_path = data_dir / output_csv
        final_df.to_csv(output_path, index=False)
        print(f"\n✅ CSV saved successfully to: {output_path}")
        print(f"📊 Total articles fetched: {total_articles}")
        print(f"⚠️  Summary - API Errors: {error_count}, Days with no data: {no_data_count}")
    except Exception as e:
        raise Exception(f"❌ Failed to save CSV file: {str(e)}")


if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.getenv("CURRENTS_API_KEY")
    
    if not API_KEY:
        raise ValueError("❌ CURRENTS_API_KEY environment variable not set")

    # Customize your range + keyword here
    fetch_news_to_csv(
        api_key="OIdnY6cCwzUywhdFhjSy2AaN3fk7jOFTjnJh59HzZvGOBsDH",
        start="2025-11-17",
        end="2025-11-18",
        keyword="technology",
        output_csv="news_output.csv"
    )