import subprocess
import pandas as pd
import requests
import os
import gzip
import shutil
from termcolor import colored
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta, datetime
import json
import time
import glob


# Constants
STORAGE_PATH = "/Volumes/rawPriceData/bybit/daily"
DATABASE_NAME = "bybit_csvs"


MARKET_TYPES = {
    "spot": "https://public.bybit.com/spot",
    "linear": "https://public.bybit.com/trading",
    "inverse": "https://public.bybit.com/trading"
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


# Returns all monthlys which are dailys that have been aggregated
def get_aggregated_monthly_records():
    sql_command = "SELECT market, trading_pair, date FROM monthly;"
    aggregated_records = run_sql_command(sql_command)
    return [tuple(line.split("\t")) for line in aggregated_records.split("\n")[1:]]


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
    # Remove '/' from the trading pair symbol
    formatted_trading_pair = trading_pair.replace('/', '')

    sql_command = f"""
    INSERT INTO daily (market, trading_pair, date, normalized, inserted_to_psql, is_delisted, first_csv)
    VALUES ('{market}', '{formatted_trading_pair}', '{date_str}', '0', '0', '0', '0');
    """
    print(colored(f"Data inserted {market}, {formatted_trading_pair}, {date_str}", 'cyan'))
    run_sql_command(sql_command)


# Update first_csv boolean to true
def update_first_csv(market, trading_pair):
    find_oldest_sql = f"""
    SELECT date FROM daily
    WHERE market = '{market}' AND trading_pair = '{trading_pair}'
    ORDER BY date ASC
    LIMIT 1;
    """
    oldest_date_result = run_sql_command(find_oldest_sql)

    # Split the result by newline and take the second element (the first one is usually empty if result starts with a newline)
    oldest_date = oldest_date_result.split('\n')[1].strip() if oldest_date_result else None

    if oldest_date:
        update_sql = f"""
        UPDATE daily
        SET first_csv = '1'
        WHERE market = '{market}' AND trading_pair = '{trading_pair}' AND date = '{oldest_date}';
        """
        run_sql_command(update_sql)
        print(colored(f"Updated first_csv for {market}, {trading_pair}, {oldest_date}", 'blue'))
    else:
        print(colored(f"No records found for {market}, {trading_pair} to update", 'red'))


# Function to scan the storage path and create a list of files
def scan_storage_for_csv_files(storage_path):
    files_info = []  # Holds information about the files
    for root, dirs, files in os.walk(storage_path):
        for file in files:
            if file.endswith('.csv'):
                # Splitting by underscore and dash to extract the symbol and date
                parts = file.split('_')
                symbol = parts[0]
                date_str = parts[1].replace('.csv', '')

                # Parse the date string into a datetime object
                try:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                except ValueError as e:
                    print(f"Error parsing date from file name {file}: {e}")
                    continue  # Skip files with invalid date formats

                rel_dir = os.path.relpath(root, storage_path)
                market = rel_dir.split(os.path.sep)[0]
                files_info.append((market, symbol, date_str))
    return files_info



#-----------------------------------------------------------------------------------------------------------#


def load_csv_to_dataframe(file_path):
    try:
        # Read the first row to determine if it's a header
        with open(file_path, 'r') as file:
            first_line = file.readline()
            if first_line.split(',')[0].isdigit():
                # If the first cell is a number, no header
                header_option = None
            else:
                # If the first cell is not a number, treat as header
                header_option = 0

        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_path, header=header_option)

        # Ensure that the DataFrame has at least four columns
        if len(df.columns) >= 4:
            # Save the second column (which could be 'timestamp' or another based on CSV structure)
            second_column = df.iloc[:, 1]

            # Drop the second column from its original position
            df.drop(df.columns[1], axis=1, inplace=True)

            # Insert the second column into the fourth position
            df.insert(3, 'NewCol', second_column)

            # Modify the fifth column based on 'sell' or 'buy'
            df[df.columns[4]] = df.iloc[:, 4].apply(lambda x: True if x == 'sell' else False)

            # Overwrite the existing CSV file with the modified DataFrame
            df.to_csv(file_path, index=False, header=bool(header_option))

        else:
            print("DataFrame does not have enough columns to shift.")

        return df
    except Exception as e:
        print(f"Failed to load and modify CSV: {e}")
        return None


def get_first_csv_daily(market, symbol):
    sql_command = f"""
    SELECT date FROM daily
    WHERE market = '{market}' AND trading_pair = '{symbol}' AND first_csv = '1'
    ORDER BY date ASC
    LIMIT 1;
    """
    result = run_sql_command(sql_command)
    return result.split('\n')[1].strip() if result else None


def get_first_csv_monthly():
    sql_command = "SELECT date FROM monthly WHERE first_csv = '1' ORDER BY date ASC LIMIT 1;"
    result = run_sql_command(sql_command)
    return result.split('\n')[1].strip() if result else None


# Function to get the earliest 'first_csv' date from both daily and monthly datasets
def get_all_first_csv_dates(market, symbol):
    daily_first_csv_date = get_first_csv_daily(market, symbol)
    monthly_first_csv_date = get_first_csv_monthly()

    # Parse dates and find the earliest
    daily_date = datetime.strptime(daily_first_csv_date, '%Y-%m-%d') if daily_first_csv_date else None
    monthly_date = datetime.strptime(monthly_first_csv_date, '%Y-%m') if monthly_first_csv_date else None

    # Return the earliest date
    if daily_date and monthly_date:
        return min(daily_date, monthly_date).strftime('%Y-%m-%d')
    elif daily_date:
        return daily_date.strftime('%Y-%m-%d')
    elif monthly_date:
        return monthly_date.strftime('%Y-%m-%d')
    else:
        return None


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


def get_all_symbols(category):
    """Fetch all tickers for a given category using the provided API endpoint."""
    url = f"https://api-testnet.bybit.com/v5/market/tickers?category={category}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if 'retCode' in data and data['retCode'] == 0:
                if 'result' in data and 'list' in data['result']:
                    symbols = [item['symbol'] for item in data['result']['list']]
                    return sorted(symbols)  # Sort the symbols alphabetically
                else:
                    print(f"Unexpected JSON structure in 'result': {data}")
            else:
                print(f"API returned error: {data.get('retMsg', 'No error message')}")
        else:
            print(f"Unsuccessful response from server: {response.status_code}")
            print(response.text)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    except json.JSONDecodeError as json_error:
        print(f"Failed to parse JSON: {json_error}")

    return None  # Return None in case of any failure

# Fetch and sort tickers for each category
categories = ["spot", "linear", "inverse"]
all_tickers = {category: get_all_symbols(category) for category in categories}

print(colored("All Tickers for BYBIT:", 'magenta'))
for category, symbols in all_tickers.items():
    print(f"{category}: {symbols}")

# Convert to JSON string if needed
json_output = json.dumps(all_tickers, indent=4)


#-----------------------------------------------------------------------------------------------------------#


def download_file(symbol, market, date_str):

    # Check if the file has already been downloaded by querying the database
    downloaded_records = get_downloaded_records_from_db()
    record = (market, symbol, date_str)

    if record in [(r[0], r[1], r[2]) for r in downloaded_records]:
        print(f"Data for {symbol} on {date_str} has already been downloaded.")
        return "Data already downloaded"


    file_url = f"{MARKET_TYPES[market]}/{symbol}/{symbol}_{date_str}.csv.gz"
    file_path = os.path.join(STORAGE_PATH, market, symbol, f"{symbol}_{date_str}.csv")

    # Check if the uncompressed file already exists to avoid re-downloading
    if os.path.exists(file_path):
        print(f"Data for {symbol} on {date_str} has already been downloaded and extracted.")
        return "Failed to download data"

    # Ensure directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Download the gzip file
    response = requests.get(file_url, stream=True)
    if response.status_code == 200:
        gzip_file_path = file_path + '.gz'
        with open(gzip_file_path, 'wb') as f:
            f.write(response.content)

        # Decompress the gzip file
        with gzip.open(gzip_file_path, 'rb') as f_in:
            with open(file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        os.remove(gzip_file_path)
        print(colored(f"Extracted and removed ZIP file for {symbol} {date_str}", 'green'))


        # Load CSV into DataFrame, remove header, and shift column
        df = load_csv_to_dataframe(file_path)


        # Insert a record into the database
        insert_new_file_record(market, symbol, date_str)
        return "Data downloaded and saved"
    else:
        print(colored(f"No data found at {file_url} (HTTP status code: {response.status_code})", 'red'))
        return "No new data"


#-----------------------------------------------------------------------------------------------------------#


# Function to process the symbols with threading
def process_symbols(market_type, executor):
    symbols = get_all_symbols(market_type)
    aggregated_monthly_records = get_aggregated_monthly_records()
    aggregated_months = {(record[0], record[1], record[2][:7]) for record in aggregated_monthly_records}

    for symbol in symbols:
        earliest_first_csv_date_str = get_all_first_csv_dates(market_type, symbol.replace("/", ""))
        earliest_first_csv_date = datetime.strptime(earliest_first_csv_date_str, '%Y-%m-%d') if earliest_first_csv_date_str else None
        current_date = datetime.combine(date.today() - timedelta(days=1), datetime.min.time())
        no_new_data_days = 0
        downloaded_dates = set(get_downloaded_dates_for_symbol(market_type, symbol.replace("/", "")))

        while True:
            date_str = current_date.strftime('%Y-%m-%d')
            month_str = current_date.strftime('%Y-%m')

            # Skip processing for dates earlier than the earliest first_csv date
            if earliest_first_csv_date and current_date < earliest_first_csv_date:
                break

            if (market_type, symbol.replace("/", ""), month_str) in aggregated_months:
                print(f"Skipping {date_str} for {symbol} as monthly data already aggregated.")
                current_date = current_date.replace(day=1) - timedelta(days=1)
                continue

            if date_str not in downloaded_dates:
                retry_count = 0
                while retry_count < 2:
                    result = download_file(symbol, market_type, date_str)

                    if result == "No new data":
                        print('a.1')
                        time.sleep(3)  # Wait for 3 seconds before retrying
                        retry_count += 1

                    elif result == "Data downloaded and saved":
                        print('b')
                        no_new_data_days = 0
                        downloaded_dates.add(date_str)
                        break

                    elif result == "Failed to download data":
                        time.sleep(3)  # Wait for 3 seconds
                        print('c')
                        result = download_file(symbol, market_type, date_str)  # Retry download
                        if result == "Data downloaded and saved":
                            no_new_data_days = 0
                            downloaded_dates.add(date_str)
                            break
                        elif result == "No new data":
                            print('a.2')
                            retry_count += 1

                    else:
                        print('d')
                        break

                if retry_count == 2:
                    no_new_data_days += 1
                    if no_new_data_days >= 3:
                        # Update the first CSV flag if three consecutive days with no new data
                        update_first_csv(market_type, symbol.replace("/", ""))
                        break

            current_date -= timedelta(days=1)

            if current_date < datetime.strptime('2000-01-01', '%Y-%m-%d'):  # Safety check
                break


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
