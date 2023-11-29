import subprocess
import pandas as pd
import os
import glob
import requests
import zipfile
import time
from termcolor import colored
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta, datetime

# Constants
STORAGE_PATH = "/Volumes/rawPriceData/kraken/daily"
DATABASE_NAME = "kraken_csvs"
BASE_URL = "https://api.kraken.com/0/public"


MARKET_TYPES = {
    "spot": "https://api.kraken.com/0/public"
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
    formatted_trading_pair = trading_pair.replace('/', '')

    sql_command = f"""
    INSERT INTO daily (market, trading_pair, date, normalized, inserted_to_psql, is_delisted, first_csv)
    VALUES ('{market}', '{formatted_trading_pair}', '{date_str}', '0', '0', '0', '0');
    """

    try:
        run_sql_command(sql_command)
        print(colored(f"Data inserted {market}, {formatted_trading_pair}, {date_str}", 'cyan'))
    except subprocess.CalledProcessError as e:
        if "Duplicate entry" in e.stderr:
            print(colored(f"Duplicate entry for {market}, {formatted_trading_pair}, {date_str}. Skipping insertion.", 'yellow'))
        else:
            raise




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
                parts = file.replace('-trades', '').split('-')
                symbol = parts[0]
                date_str = '-'.join(parts[1:])
                date_obj = datetime.strptime(date_str.replace('.csv', ''), '%Y-%m-%d')
                rel_dir = os.path.relpath(root, storage_path)
                market = rel_dir.split(os.path.sep)[0]
                files_info.append((market, symbol, date_str.replace('.csv', '')))
    return files_info


#-----------------------------------------------------------------------------------------------------------#


def get_first_csv_daily(market, symbol):
    sql_command = f"""
    SELECT date FROM daily
    WHERE market = '{market}' AND trading_pair = '{symbol}' AND first_csv = '1'
    ORDER BY date ASC
    LIMIT 1;
    """
    result = run_sql_command(sql_command)
    return result.split('\n')[1].strip() if result else None


def get_first_csv_monthly(market, symbol):
    sql_command = f"""SELECT date FROM monthly
    WHERE market = '{market}' AND trading_pair = '{symbol}' AND first_csv = '1'
    ORDER BY date ASC
    LIMIT 1;"""
    result = run_sql_command(sql_command)
    return result.split('\n')[1].strip() if result else None


# Function to get the earliest 'first_csv' date from both daily and monthly datasets
def get_all_first_csv_dates(market, symbol):
    daily_first_csv_date = get_first_csv_daily(market, symbol)
    monthly_first_csv_date = get_first_csv_monthly(market, symbol)

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


def record_exists_in_db(market, trading_pair, date_str):
    sql_command = f"SELECT COUNT(*) FROM daily WHERE market = '{market}' AND trading_pair = '{trading_pair}' AND date = '{date_str}';"
    result = run_sql_command(sql_command)
    count = int(result.split("\n")[1].strip())  # Parse the count from the command output
    return count > 0


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
    url = BASE_URL + "/AssetPairs"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()['result']
        formatted_pairs = []

        for pair_name, pair_info in data.items():
            if pair_name.endswith('.d'):
                continue

            base = pair_info.get('base').replace('XBT', 'BTC')
            quote = pair_info.get('quote').replace('XBT', 'BTC')

            if 'wsname' in pair_info:
                formatted_pair = pair_info['wsname'].replace('XBT', 'BTC')
            else:
                formatted_pair = f"{base}/{quote}"

            formatted_pairs.append(formatted_pair)

        print("Retrieved Kraken Spot Products:", formatted_pairs)
        return formatted_pairs
    else:
        print(f"Failed to retrieve spot data: {response.status_code}")
        return None



#-----------------------------------------------------------------------------------------------------------#


def download_file(symbol, market, date_str, max_retries=3):
    downloaded_records = get_downloaded_records_from_db()
    record = (market, symbol, date_str)

    if record in [(r[0], r[1], r[2]) for r in downloaded_records]:
        return "Data already downloaded"

    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    unix_timestamp_start = int(date_obj.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    unix_timestamp_end = unix_timestamp_start + 86400  # End of day

    dir_path = os.path.join(STORAGE_PATH, market, symbol.replace("/", ""))
    os.makedirs(dir_path, exist_ok=True)
    csv_file_path = os.path.join(dir_path, f"{symbol.replace('/', '')}-trades-{date_str}.csv")

    since = unix_timestamp_start
    all_trades_collected = False
    trades_data = []

    api_symbol = symbol.replace('XBT', 'BTC')

    while not all_trades_collected and max_retries > 0:
        try:
            response = requests.get(f"{BASE_URL}/Trades?pair={api_symbol}&since={since}")
            if response.status_code == 200:
                data = response.json()
                if 'result' in data and data['result'][symbol]:
                    trades = data['result'][symbol]
                    last = int(data['result']['last'])

                    df = pd.DataFrame(trades, columns=['price', 'volume', 'time', 'buy_sell', 'market_limit', 'misc', 'trade_id'])
                    df['time'] = df['time'].apply(lambda x: int(float(x) * 1000))

                    # Filter trades within the specific day and append to the list
                    df_day = df[(df['time'] >= unix_timestamp_start * 1000) & (df['time'] <= unix_timestamp_end * 1000)]
                    trades_data.append(df_day)

                    # Check if the last trade in the batch is beyond the end of the day
                    if df['time'].max() >= unix_timestamp_end * 1000:
                        all_trades_collected = True
                    else:
                        since = last
                else:
                    all_trades_collected = True
            else:
                print(f"Error fetching data for {symbol} on {date_str}: {response.status_code}")
                max_retries -= 1
        except Exception as e:
            print(f"Attempt {max_retries}: Error encountered while downloading data for {symbol} on {date_str}: {e}")
            max_retries -= 1

    # Process and save all trades for the day
    if trades_data:
        full_day_trades = pd.concat(trades_data, ignore_index=True)

        # Check if the concatenated DataFrame is empty
        if full_day_trades.empty:
            print(colored(f"No new trades for {symbol} on {date_str}, creating placeholder CSV.", 'magenta'))
            full_day_trades.to_csv(csv_file_path, index=False, header=False)

            # Check if the record already exists in the database
            if not record_exists_in_db(market, symbol, date_str):
                insert_new_file_record(market, symbol, date_str)
            else:
                print(colored(f"Record already exists for {market}, {symbol}, {date_str}, skipping insertion.", 'blue'))

            return "No new data"

        else:
            # Process and save data
            full_day_trades['isbuyermaker'] = (full_day_trades['buy_sell'] == 'b') & (full_day_trades['market_limit'] == 'l') | (full_day_trades['buy_sell'] == 's') & (full_day_trades['market_limit'] == 'm')
            full_day_trades.drop(['buy_sell', 'market_limit', 'misc'], axis=1, inplace=True)
            full_day_trades.rename(columns={'volume': 'qty'}, inplace=True)

            # Rearrange columns to put 'trade_id' first
            cols = ['trade_id'] + [col for col in full_day_trades.columns if col != 'trade_id']
            full_day_trades = full_day_trades[cols]

            full_day_trades.to_csv(csv_file_path, index=False, header=False)

            # Insert a record into the database
            print(colored(f"Completed downloading data for {symbol} on {date_str}", 'green'))
            insert_new_file_record(market, symbol, date_str)
            return "Data downloaded and saved"
    else:
        # No trades data was collected, return "Failed to download data"
        print(f"Failed to download data for {symbol} on {date_str}.")
        return "Failed to download data"


#-----------------------------------------------------------------------------------------------------------#


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

    # Returns all monthly csv files
    aggregated_monthly_records = get_aggregated_monthly_records()

    # Delete records from the database that are not present in storage
    for record in downloaded_records_db:
        if (record[0], record[1], record[2]) not in [(f[0], f[1], f[2]) for f in files_in_storage]:
            delete_record_from_db(*record)

    # Insert new records for files present in storage but not in the database
    for file_info in files_in_storage:
        if (file_info[0], file_info[1], file_info[2]) not in [(r[0], r[1], r[2]) for r in downloaded_records_db]:
            insert_new_file_record(*file_info)

    # Use threading to download files
    with ThreadPoolExecutor(max_workers=1) as executor:
        for market_type in MARKET_TYPES.keys():
            process_symbols(market_type, executor)
