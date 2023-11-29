import subprocess
import os
import requests
import zipfile
from termcolor import colored
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta, datetime

# Constants
STORAGE_PATH = "/Volumes/rawPriceData/binance/daily"
DATABASE_NAME = "binance_csvs"
BASE_URL = "https://data.binance.vision/data"

MARKET_TYPES = {
    "spot": "https://api.binance.com/api/v3/exchangeInfo",
    "usdm": "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "coinm": "https://dapi.binance.com/dapi/v1/exchangeInfo"
}

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
        print("MySQL error:", e.stderr)
        raise  # Optionally re-raise the error if you want to stop the script


#-----------------------------------------------------------------------------------------------------------#


# Function to get the list of downloaded records from the database
def get_downloaded_records_from_db():
    sql_command = "SELECT market, trading_pair, date FROM daily;"
    downloaded_records = run_sql_command(sql_command)
    # Parse the output into a list of tuples (market, trading_pair, date)
    return [tuple(line.split("\t")) for line in downloaded_records.split("\n")[1:]]

# Function to delete the database record for files not found
def delete_record_from_db(market, trading_pair, date_str):
    sql_command = f"""
    DELETE FROM daily
    WHERE market = '{market}' AND trading_pair = '{trading_pair}' AND date = '{date_str}';
    """
    print(colored(f"Deleted from DB: {record}", 'yellow'))
    run_sql_command(sql_command)

# Function to insert new file records into the database
def insert_new_file_record(market, trading_pair, date_str):
    sql_command = f"""
    INSERT INTO daily (market, trading_pair, date)
    VALUES ('{market}', '{trading_pair}', '{date_str}');
    """
    print(colored(f"Data inserted {market}, {trading_pair}, {date_str}", 'cyan'))
    run_sql_command(sql_command)

# Function to scan the storage path and create a list of files
def scan_storage_for_csv_files(storage_path):
    files_info = []  # Holds information about the files
    for root, dirs, files in os.walk(storage_path):
        for file in files:
            if file.endswith('.csv'):
                parts = file.replace('-trades', '').split('-')
                symbol = parts[0]
                date_str = '-'.join(parts[1:])
                date_obj = datetime.strptime(date_str.replace('.csv', ''), '%Y-%m-%d')
                rel_dir = os.path.relpath(root, storage_path)
                market = rel_dir.split(os.path.sep)[0]
                files_info.append((market, symbol, date_str.replace('.csv', '')))
    return files_info


#-----------------------------------------------------------------------------------------------------------#


# Helper function to create a date range
def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def get_downloaded_dates_for_symbol(market, symbol):
    sql_command = f"""
    SELECT date FROM daily
    WHERE market = '{market}' AND trading_pair = '{symbol}';
    """
    result = run_sql_command(sql_command)
    return [line for line in result.split("\n")[1:]]

# Function to collect all download tasks
def collect_download_tasks():
    tasks = []
    for market_type in MARKET_TYPES.keys():
        symbols = get_all_symbols(market_type)
        for symbol in symbols:
            downloaded_dates = get_downloaded_dates_for_symbol(market_type, symbol)
            current_date = date.today().replace(day=1)
            end_date = date.today() - timedelta(days=1)
            while current_date <= end_date:
                date_str = current_date.strftime('%Y-%m-%d')
                if date_str not in downloaded_dates:
                    tasks.append((symbol, market_type, date_str))
                current_date += timedelta(days=1)
    return tasks


#-----------------------------------------------------------------------------------------------------------#


def get_all_symbols(market_type):
    print(f"Fetching symbols for {market_type} market...")
    try:
        response = requests.get(MARKET_TYPES[market_type])
        response.raise_for_status()  # Raises a HTTPError if the HTTP request returned an unsuccessful status code
        data = response.json()

        symbols = []
        if market_type == "spot":
            symbols = [symbol['symbol'] for symbol in data['symbols'] if 'status' in symbol and symbol['status'] == 'TRADING']
        elif market_type == "usdm":
            symbols = [
                symbol['symbol'] for symbol in data['symbols']
                if 'contractType' in symbol and symbol['contractType'] == 'PERPETUAL' and 'status' in symbol and symbol['status'] == 'TRADING'
            ]
        elif market_type == "coinm":
            symbols = [symbol['symbol'] for symbol in data['symbols'] if 'contractStatus' in symbol and symbol['contractStatus'] == 'TRADING']

        print(f"Found {len(symbols)} trading symbols for {market_type} market.")
        return symbols

    except requests.RequestException as e:
        print(f"Error fetching symbols for {market_type} market: {e}")
        return []
    except KeyError as e:
        print(f"KeyError encountered while processing the symbols for {market_type} market. Data: {data}")
        return []


#-----------------------------------------------------------------------------------------------------------#


def download_file(symbol, market, date_str):

    # Check if the file has already been downloaded by querying the database
    downloaded_records = get_downloaded_records_from_db()
    record = (market, symbol, date_str)

    if record in [(r[0], r[1], r[2]) for r in downloaded_records]:
        print(f"Data for {symbol} on {date_str} has already been downloaded.")
        return "Data already downloaded"

    # Determine the market URL based on the market type
    market_url_suffix = ""
    if market == "usdm":
        market_url_suffix = "futures/um"
    elif market == "coinm":
        market_url_suffix = "futures/cm"
    else:
        market_url_suffix = market  # For spot market, it stays the same

    # Construct the zip file URL
    zip_file_url = f"{BASE_URL}/{market_url_suffix}/daily/trades/{symbol}/{symbol}-trades-{date_str}.zip"
    zip_file_path = os.path.join(STORAGE_PATH, market, symbol, f"{symbol}-trades-{date_str}.zip")

    os.makedirs(os.path.dirname(zip_file_path), exist_ok=True)
    response = requests.get(zip_file_url, stream=True)


    if response.status_code == 200:
        with open(zip_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)


        # Extract the zip file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(os.path.dirname(zip_file_path))
        os.remove(zip_file_path)
        print(colored(f"Extracted and removed ZIP file for {symbol} {date_str}", 'green'))

        # Insert a record into the database
        insert_new_file_record(market, symbol, date_str)
        return "Data downloaded and database updated"
    else:
        print(colored(f"No data found at {zip_file_url} (HTTP status code: {response.status_code})", 'red'))
        return "No data"


#-----------------------------------------------------------------------------------------------------------#


# Function to process the symbols with threading
def process_symbols(market_type, executor):
    symbols = get_all_symbols(market_type)
    tasks = []
    for symbol in symbols:
        downloaded_dates = get_downloaded_dates_for_symbol(market_type, symbol)
        current_date = date.today().replace(day=1)
        end_date = date.today() - timedelta(days=1)
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            if date_str not in downloaded_dates:
                # Schedule the download task
                tasks.append(executor.submit(download_file, symbol, market_type, date_str))
            current_date += timedelta(days=1)

    # Wait for all tasks to complete
    for task in tasks:
        task.result()


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

    # Use threading to download files
    with ThreadPoolExecutor(max_workers=15) as executor:
        for market_type in MARKET_TYPES.keys():
            process_symbols(market_type, executor)
