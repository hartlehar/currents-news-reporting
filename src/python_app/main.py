import os
import news_api_utils
import db_utils

# -------------------------------
# Read config from environment variables
# -------------------------------
API_KEY     = os.getenv("API_KEY")
START_DATE  = os.getenv("START_DATE", "2025-08-01")
END_DATE    = os.getenv("END_DATE", "2025-08-02")
KEYWORD     = os.getenv("KEYWORD", "world")
CSV_NAME    = os.getenv("CSV_NAME", "output.csv")

# -------------------------------
# Helper to prompt interactively if running locally
# -------------------------------
def prompt_if_missing():
    global API_KEY, START_DATE, END_DATE, KEYWORD, CSV_NAME

    # Only prompt if running in interactive mode and any variable is missing
    try:
        if not API_KEY:
            API_KEY = input("Please enter your API key: ").strip()
        if not START_DATE:
            START_DATE = input("Please enter your start date (YYYY-MM-DD): ").strip()
        if not END_DATE:
            END_DATE = input("Please enter your end date (YYYY-MM-DD): ").strip()
        if not CSV_NAME:
            CSV_NAME = input("Please enter your output CSV filename: ").strip()
        if not KEYWORD:
            KEYWORD = input("Please enter your keyword: ").strip()
    except EOFError:
        # Running non-interactively (e.g., Docker), ignore input()
        pass

# -------------------------------
# Main execution
# -------------------------------
if __name__ == "__main__":
    
    news_api_utils.fetch_news_to_csv(
        api_key=API_KEY,
        start=START_DATE,
        end=END_DATE,
        keyword=KEYWORD,
        output_csv=CSV_NAME
    )

    print("Building SQLite database...")
    db_utils.main()  # Ensure you wrap db_utils.py logic in a main() function
    print("Done!")
