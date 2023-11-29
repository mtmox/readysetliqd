import subprocess
import os
import requests
import zipfile
import json
from termcolor import colored
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import logging

# Constants
STORAGE_PATH = "/Volumes/rawPriceData/binance/monthly"
DATABASE_NAME = "binance_csvs"
BASE_URL = "https://data.binance.vision/data"

MARKET_TYPES = {
    "spot": "https://api.binance.com/api/v3/exchangeInfo",
    "usdm": "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "coinm": "https://dapi.binance.com/dapi/v1/exchangeInfo"
}

# Setup logging
logging.basicConfig(level=logging.INFO)

# Function to run SQL commands and return the result
def run_sql_command(sql_command):
    cmd = [
        "mysql",
        "--login-path=client",
        "-e",
        sql_command,
        DATABASE_NAME
    ]
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logging.error("MySQL error: %s", e.stderr)
        raise


#-----------------------------------------------------------------------------------------------------------#


# Function to get the list of downloaded records from the database
def get_downloaded_records_from_db():
    sql_command = "SELECT market, trading_pair, date FROM monthly;"
    downloaded_records = run_sql_command(sql_command)
    if downloaded_records:
        # Parse the output into a list of tuples (market, trading_pair, date)
        return [tuple(line.split("\t")) for line in downloaded_records.split("\n")[1:] if line]
    else:
        logging.info("No records fetched from the database.")
        return []


# Function to delete the database record for files not found
def delete_record_from_db(market, trading_pair, date_str):
    sql_command = f"""
    DELETE FROM monthly
    WHERE market = '{market}' AND trading_pair = '{trading_pair}' AND date = '{date_str}';
    """
    run_sql_command(sql_command)
    print(colored(f"Deleted from DB: {record}", 'yellow'))


# Function to insert new file records into the database
def insert_new_file_record(market, trading_pair, date_str):
    sql_command = f"""
    INSERT INTO monthly (market, trading_pair, date)
    VALUES ('{market}', '{trading_pair}', '{date_str}');
    """
    run_sql_command(sql_command)
    print(colored(f"Inserted into DB: {market}, {trading_pair}, {date_str}", 'cyan'))

# Function to scan the storage path and create a list of files
def scan_storage_for_csv_files(storage_path):
    files_info = []  # Holds information about the files
    try:
        for root, dirs, files in os.walk(storage_path):
            for file in files:
                if file.endswith('.csv'):
                    parts = file.replace('-trades', '').split('-')
                    symbol = parts[0]
                    date_str = '-'.join(parts[1:])
                    date_obj = datetime.strptime(date_str.replace('.csv', ''), '%Y-%m')
                    rel_dir = os.path.relpath(root, storage_path)
                    market = rel_dir.split(os.path.sep)[0]
                    files_info.append((market, symbol, date_str.replace('.csv', '')))
        return files_info
    except Exception as e:
        logging.error("Error scanning storage: %s", e)
        return []



#-----------------------------------------------------------------------------------------------------------#


def mark_earliest_existing_csv_as_first(symbol, market):
    # Retrieve the earliest date for the symbol from the database
    earliest_date_command = """
    SELECT MIN(date) FROM monthly
    WHERE trading_pair = '{}' AND market = '{}';
    """.format(symbol, market)
    earliest_date_result = run_sql_command(earliest_date_command).split("\n")
    if len(earliest_date_result) < 2 or earliest_date_result[1] == 'NULL':
        print(f"No existing data found to mark as first_csv for {symbol} in {market} market.")
        return

    earliest_date = earliest_date_result[1]

    # Update the first_csv flag for the earliest date
    mark_first_csv_command = """
    UPDATE monthly
    SET first_csv = 1
    WHERE trading_pair = '{}' AND market = '{}' AND date = '{}';
    """.format(symbol, market, earliest_date)
    run_sql_command(mark_first_csv_command)
    print(colored(f"Marked CSV of date {earliest_date} as first for {symbol} in {market} market.", 'yellow'))



def mark_first_csv_based_on_downloaded_data(symbol, market):
    print(colored(f"Marking first CSV for {symbol} in {market} after finding data.", 'yellow'))

    # First, get the minimum date with downloaded data
    min_date_command = f"""
    SELECT MIN(date) FROM monthly
    WHERE trading_pair = '{symbol}' AND market = '{market}';
    """
    min_date_result = run_sql_command(min_date_command).split("\n")
    min_date = min_date_result[1] if len(min_date_result) > 1 else None

    if min_date:
        # Update the first_csv flag using the retrieved minimum date
        mark_first_csv_command = f"""
        UPDATE monthly
        SET first_csv = 1
        WHERE trading_pair = '{symbol}' AND market = '{market}'
        AND date = '{min_date}';
        """
        run_sql_command(mark_first_csv_command)


# Helper function to check if a record already exists
def record_exists(market, symbol, date_str):
    sql_command = f"""
    SELECT COUNT(*) FROM monthly
    WHERE market = '{market}' AND trading_pair = '{symbol}' AND date = '{date_str}';
    """
    result = run_sql_command(sql_command)
    count = int(result.split("\n")[1])
    return count > 0

#-----------------------------------------------------------------------------------------------------------#


def first_csv(symbol, market):
    print(colored(f"Processing first_csv for {symbol} in {market} market.", 'magenta'))

    # Check if first_csv is already marked
    first_csv_check_command = """
    SELECT first_csv FROM monthly
    WHERE trading_pair = '{}' AND market = '{}'
    ORDER BY date ASC
    LIMIT 1;
    """.format(symbol, market)
    first_csv_check_result = run_sql_command(first_csv_check_command).split("\n")

    # If first_csv is already marked, skip processing this symbol
    if len(first_csv_check_result) >= 2 and first_csv_check_result[1] == '1':
        return

    # Get the earliest date for the symbol from the database
    oldest_date_command = """
    SELECT MIN(date) FROM monthly
    WHERE trading_pair = '{}' AND market = '{}';
    """.format(symbol, market)
    oldest_date_result = run_sql_command(oldest_date_command).split("\n")
    has_existing_data = len(oldest_date_result) >= 2 and oldest_date_result[1] != 'NULL'

    # Determine start date for downloading data
    current_date = datetime.now() - relativedelta(months=1)
    current_year, current_month = current_date.year, current_date.month

    # Flag to indicate if any new data has been downloaded
    newly_downloaded = False

    # Iterate backwards starting from the current month minus one
    while current_year >= 2017:
        download_result = download_file(symbol, market, current_year, current_month)

        if download_result == "Data downloaded":
            newly_downloaded = True
            insert_new_file_record(market, symbol, f"{current_year}-{current_month:02}")

        elif download_result == "No data":
            # If we have existing data and no new data downloaded, find the earliest existing file and mark it
            if has_existing_data and not newly_downloaded:
                mark_earliest_existing_csv_as_first(symbol, market)
                break

            # If new data has been downloaded, mark the earliest downloaded file
            if newly_downloaded:
                mark_first_csv_based_on_downloaded_data(symbol, market)
                break

        # Move to the previous month
        current_month -= 1
        if current_month < 1:
            current_month = 12
            current_year -= 1

    print(colored(f"Completed processing first_csv for {symbol} in {market} market.", 'magenta'))


#-----------------------------------------------------------------------------------------------------------#


def fill_gaps(symbol, market):
    print(colored(f"Starting to fill gaps for {symbol} in {market} market.", 'blue'))

    # Get the first CSV date
    first_csv_date_command = """
    SELECT MIN(date) FROM monthly
    WHERE trading_pair = '{}' AND market = '{}' AND first_csv = 1;
    """.format(symbol, market)
    first_csv_date = run_sql_command(first_csv_date_command).split("\n")[1]

    # Handle case where first_csv_date is not available
    if first_csv_date == 'NULL' or not first_csv_date:
        logging.warning(f"No first CSV date found for {symbol} in {market}.")
        first_csv_date = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m')

    # Get the most recent CSV date
    recent_csv_date_command = """
    SELECT MAX(date) FROM monthly
    WHERE trading_pair = '{}' AND market = '{}';
    """.format(symbol, market)
    recent_csv_date = run_sql_command(recent_csv_date_command).split("\n")[1]

    # Handle case where recent_csv_date is not available
    if recent_csv_date == 'NULL' or not recent_csv_date:
        logging.warning(f"No recent CSV date found for {symbol} in {market}.")
        recent_csv_date = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m')

    # Convert dates to datetime objects
    first_date = datetime.strptime(first_csv_date, '%Y-%m')
    recent_date = datetime.strptime(recent_csv_date, '%Y-%m')

    # Fill in the gaps
    current_date = first_date
    while current_date <= recent_date:
        year_month_str = current_date.strftime('%Y-%m')
        logging.info(f"Checking for {symbol} in {market} for date {year_month_str}.")
        if not record_exists(market, symbol, year_month_str):
            download_result = download_file(symbol, market, current_date.year, current_date.month)
            if download_result == "Data downloaded":
                insert_new_file_record(market, symbol, year_month_str)
                logging.info(f"Downloaded and inserted record for {symbol} {year_month_str} in {market}.")
            elif download_result == "No data":
                logging.info(f"No data available for {symbol} {year_month_str} in {market}.")
        current_date += relativedelta(months=1)

    # Check for new data
    while current_date < datetime.now() - relativedelta(months=1):
        year_month_str = current_date.strftime('%Y-%m')
        logging.info(f"Checking for new data for {symbol} in {market} for date {year_month_str}.")
        if not record_exists(market, symbol, year_month_str):
            download_result = download_file(symbol, market, current_date.year, current_date.month)
            if download_result == "Data downloaded":
                insert_new_file_record(market, symbol, year_month_str)
                logging.info(f"Downloaded and inserted new record for {symbol} {year_month_str} in {market}.")
            elif download_result == "No data":
                logging.info(f"No new data available for {symbol} {year_month_str} in {market}. Breaking loop.")
                break
        current_date += relativedelta(months=1)

    print(colored(f"Completed filling gaps for {symbol} in {market} market.", 'blue'))


#-----------------------------------------------------------------------------------------------------------#


# Function to get all symbols
def get_all_symbols(market_type):
    print(f"Fetching symbols for {market_type} market...")
    try:
        # Fetch symbols from API
        response = requests.get(MARKET_TYPES[market_type])
        response.raise_for_status()
        data = response.json()

        fetched_symbols = []
        if market_type == "spot":
            fetched_symbols = [symbol['symbol'] for symbol in data['symbols']]
        elif market_type == "usdm":
            fetched_symbols = [symbol['symbol'] for symbol in data['symbols'] if 'contractType' in symbol and symbol['contractType'] == 'PERPETUAL']
        elif market_type == "coinm":
            fetched_symbols = [symbol['symbol'] for symbol in data['symbols']]


        print(f"Found {len(fetched_symbols)} trading symbols for {market_type} market.")
        return fetched_symbols

    except requests.RequestException as e:
        print(f"Error fetching symbols for {market_type}: {e}")
        return []
    except KeyError as e:
        print(f"KeyError encountered while fetching symbols for {market_type}. Data may be missing or structured differently than expected.")
        return []
    except FileNotFoundError:
        print("JSON file with manual symbols not found.")
        return fetched_symbols  # Return only API fetched symbols if JSON file is not found


#-----------------------------------------------------------------------------------------------------------#


def download_file(symbol, market, year, month):
    csv_file_name = f"{symbol}-trades-{year}-{month:02}.csv"
    zip_file_dir = os.path.join(STORAGE_PATH, market, symbol)
    zip_file_path = os.path.join(zip_file_dir, f"{symbol}-trades-{year}-{month:02}.zip")

    if market == "usdm":
        market_url = "futures/um"
    elif market == "coinm":
        market_url = "futures/cm"
    else:
        market_url = market

    # Ensure the directory for the zip file exists
    os.makedirs(zip_file_dir, exist_ok=True)

    if os.path.isfile(os.path.join(zip_file_dir, csv_file_name)):
        print(f"Data already downloaded for {symbol} {year}-{month}.")
        return "Data already downloaded"

    url = f"{BASE_URL}/{market_url}/monthly/trades/{symbol}/{symbol}-trades-{year}-{month:02}.zip"
    print(f"URL for download: {url}")

    try:
        response = requests.get(url, stream=True)
        print(f"Response status code: {response.status_code}")
        print(f"Response Content-Type: {response.headers.get('Content-Type')}")

        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '')
            print(f"Content-Type: {content_type}")

            with open(zip_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            if zipfile.is_zipfile(zip_file_path):
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    zip_ref.extractall(zip_file_dir)
                os.remove(zip_file_path)
                print(colored(f"Extracted and deleted zip file for {symbol} {year}-{month}", 'green'))
                return "Data downloaded"
            else:
                print(f"Invalid ZIP file: {zip_file_path}")
                os.remove(zip_file_path)
                return "Invalid zip file" if response.status_code != 200 else "No data"
        else:
            print(colored(f"No ZIP data found for {symbol} {year}-{month}, response code {response.status_code}", 'red'))
            return "No data"
    except requests.RequestException as e:
        print(f"Request error for {symbol} {year}-{month}: {e}")
        return "Request failed"
    except zipfile.BadZipFile:
        print(f"Bad zip file for {symbol} {year}-{month}, file path: {zip_file_path}")
        return "Bad zip file"
    except Exception as e:
        print(f"Unexpected error for {symbol} {year}-{month}: {e}")
        return "Error"


#-----------------------------------------------------------------------------------------------------------#


# Main logic
if __name__ == "__main__":
      # Get the list of downloaded files from the storage
      files_in_storage = scan_storage_for_csv_files(STORAGE_PATH)

      # Get the list of downloaded records from the database
      downloaded_records_db = get_downloaded_records_from_db()

      # Delete records from the database that are not present in storage
      for record in downloaded_records_db:
          if (record[0], record[1], record[2]) not in [(f[0], f[1], f[2]) for f in files_in_storage]:
              delete_record_from_db(*record)


      # Insert new records for files present in storage but not in the database
      for file_info in files_in_storage:
          if (file_info[0], file_info[1], file_info[2]) not in [(r[0], r[1], r[2]) for r in downloaded_records_db]:
              insert_new_file_record(*file_info)


      # Process symbols for each market type
      logging.info("Starting the CSV download and gap filling process.")
      for market_type in MARKET_TYPES.keys():
          symbols = get_all_symbols(market_type)
          for symbol in symbols:
              first_csv(symbol, market_type)
              fill_gaps(symbol, market_type)
      logging.info("Process completed.")
