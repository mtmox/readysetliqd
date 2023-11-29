#!/opt/homebrew/bin/python3
import requests
import pandas as pd
import os
import subprocess
from datetime import datetime
from termcolor import colored
from dotenv import load_dotenv

# Load the environment variables from your profile
os.system('source ~/.bash_profile')

# Constants
STORAGE_PATH = "/Volumes/rawPriceData/bitstamp/daily"
DATABASE_NAME = "bitstamp_csvs"
BASE_URL = "https://www.bitstamp.net"
MARKET_TYPES = {"spot"}


load_dotenv()  # Load environment variables from .env file

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')


# Function to run SQL commands and return the result
def run_sql_command(sql_command):
    mysql_path = "/opt/homebrew/bin/mysql"
    cmd = [
        mysql_path,
        "-h", MYSQL_HOST,
        "-u", MYSQL_USER,
        "-p" + MYSQL_PASSWORD,  # Note: no space between -p and password
        "-e", sql_command,
        MYSQL_DATABASE
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



def delete_record_from_db(market, trading_pair, date_str):
    sql_command = f"""
    DELETE FROM daily
    WHERE market = '{market}' AND trading_pair = '{trading_pair}' AND date = '{date_str}';
    """
    try:
        result = run_sql_command(sql_command)
        print(colored(f"Deleted from DB: {market}, {trading_pair}, {date_str}. Result: {result}", 'yellow'))
    except subprocess.CalledProcessError as e:
        print(f"Error deleting record for {market}, {trading_pair}, {date_str}: {e.stderr}")


def insert_new_file_record(market, trading_pair, date_str):
    formatted_trading_pair = trading_pair.replace('/', '')
    sql_command = f"""
    INSERT INTO daily (market, trading_pair, date, normalized, inserted_to_psql, is_delisted, first_csv)
    VALUES ('{market}', '{formatted_trading_pair}', '{date_str}', '0', '0', '0', '0');
    """
    try:
        result = run_sql_command(sql_command)
        print(colored(f"Data inserted: {market}, {formatted_trading_pair}, {date_str}. Result: {result}", 'cyan'))
    except subprocess.CalledProcessError as e:
        print(f"Error inserting record for {market}, {formatted_trading_pair}, {date_str}: {e.stderr}")



# Function to scan the storage path and create a list of files
def scan_storage_for_csv_files(storage_path):
    files_info = []
    for root, dirs, files in os.walk(storage_path):
        for file in files:
            if file.endswith('.csv'):
                try:
                    # Extracting symbol, date, and time from the filename
                    parts = file.split('-')
                    symbol = parts[0]
                    datetime_str = '-'.join(parts[2:]).replace('.csv', '')

                    # Correct format string to match the filename structure
                    date_obj = datetime.strptime(datetime_str, '%Y-%m-%d-%H%M')
                    date_str = date_obj.strftime('%Y-%m-%d %H:%M')

                    # Constructing file info
                    rel_dir = os.path.relpath(root, storage_path)
                    market = rel_dir.split(os.path.sep)[0]
                    files_info.append((market, symbol, date_str))
                except ValueError as e:
                    print(f"Error parsing date and time from filename: {file}. Error: {e}")
                    continue
    return files_info


#-----------------------------------------------------------------------------------------------------------#


def get_all_symbols(market_type):
    url = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
    response = requests.get(url)
    if response.status_code == 200:
        trading_pairs_info = response.json()
        # Fetch all symbols where trading is enabled
        symbols = [pair['url_symbol'] for pair in trading_pairs_info if pair['trading'] == "Enabled"]
        return symbols
    else:
        print(f"Failed to retrieve data: {response.status_code}")
        print("Response:", response.text)
        return None


def fetch_data(symbol, time_frame):
    url = f"{BASE_URL}/api/v2/transactions/{symbol}/?time={time_frame}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(colored(f"Failed to fetch data for {symbol} ({time_frame}): {response.status_code}", 'red'))
        return None


def process_and_store_data(data, symbol, time_frame, market):
    # Capitalize the symbol for the folder name
    capitalized_symbol = symbol.upper()

    # Format the current date and time
    current_datetime = datetime.now().strftime('%Y-%m-%d-%H%M')

    # Create a directory path including market and capitalized symbol
    dir_path = os.path.join(STORAGE_PATH, market, capitalized_symbol)
    os.makedirs(dir_path, exist_ok=True)

    # Define the file path with the current date and time
    csv_file_path = os.path.join(dir_path, f"{capitalized_symbol}-trades-{current_datetime}.csv")

    # Create DataFrame from data
    df = pd.DataFrame(data)

    # Check if DataFrame has at least four columns
    if df.shape[1] >= 4:
        # Reorder columns: 4th, 3rd, 1st, 2nd, 5th, 6th, ...
        new_order = [3, 2] + list(range(df.shape[1]))  # Creating a list of new column indices
        new_order = sorted(set(new_order), key=new_order.index)  # Remove duplicates while preserving order

        df = df.iloc[:, new_order]

    # Save data to CSV without header
    df.to_csv(csv_file_path, index=False, header=False)
    print(colored(f"Data saved to {csv_file_path}", 'green'))


#-----------------------------------------------------------------------------------------------------------#


# Main logic
if __name__ == "__main__":

    print("Main logic started")
    files_in_storage = scan_storage_for_csv_files(STORAGE_PATH)
    print(f"Files in storage: {files_in_storage}")

    downloaded_records_db = get_downloaded_records_from_db()
    print(f"Downloaded records from DB: {downloaded_records_db}")

    # Delete records from the database that are not present in storage
    for record in downloaded_records_db:
        if (record[0], record[1], record[2]) not in [(f[0], f[1], f[2]) for f in files_in_storage]:
            delete_record_from_db(*record)

    # Insert new records for files present in storage but not in the database
    for file_info in files_in_storage:
        if (file_info[0], file_info[1], file_info[2]) not in [(r[0], r[1], r[2]) for r in downloaded_records_db]:
            insert_new_file_record(*file_info)

    for market_type in MARKET_TYPES:
        symbols = get_all_symbols(market_type)
        print(f"Fetched symbols for {market_type}: {symbols}")
        if symbols:
            for symbol in symbols:
                time_frame = 'day'
                data = fetch_data(symbol, time_frame)
                if data:
                    process_and_store_data(data, symbol, time_frame, market_type)
