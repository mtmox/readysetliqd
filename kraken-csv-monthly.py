import subprocess
import pandas as pd
import os
import glob
import requests
import zipfile
import calendar
from termcolor import colored
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta, datetime

# Constants
STORAGE_PATH_DAILY = "/Volumes/rawPriceData/kraken/daily"
STORAGE_PATH_MONTHLY = "/Volumes/rawPriceData/kraken/monthly"
DATABASE_NAME = "kraken_csvs"


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
        print("Error executing SQL command:", e)
        print("Command:", e.cmd)
        print("Return Code:", e.returncode)
        print("Output:", e.output)
        print("Error Output:", e.stderr)
        raise


#-----------------------------------------------------------------------------------------------------------#


# Function to get the list of downloaded records from the database
def get_downloaded_records_from_db_monthly():
    sql_command = "SELECT market, trading_pair, date FROM monthly;"
    downloaded_records = run_sql_command(sql_command)
    if downloaded_records:
        # Parse the output into a list of tuples (market, trading_pair, date)
        return [tuple(line.split("\t")) for line in downloaded_records.split("\n")[1:] if line]
    else:
        print("a")
        return []


# Function to get the list of downloaded records from the database
def get_downloaded_records_from_db_daily():
    sql_command = "SELECT market, trading_pair, date, first_csv FROM daily;"
    downloaded_records = run_sql_command(sql_command)
    if downloaded_records:
        # Parse the output into a list of tuples (market, trading_pair, date, first_csv)
        return [tuple(line.split("\t")) for line in downloaded_records.split("\n")[1:] if line]
    else:
        print("No daily records found.")
        return []



# Function to delete the database record for files not found
def delete_record_from_db_monthly(market, trading_pair, date_str):
    sql_command = f"""
    DELETE FROM monthly
    WHERE market = '{market}' AND trading_pair = '{trading_pair}' AND date = '{date_str}';
    """
    run_sql_command(sql_command)
    print(colored(f"Deleted from DB: {record}", 'yellow'))


# Function to delete the database record for files not found
def delete_record_from_db_daily(market, trading_pair, month):
    # Assuming 'month' is in 'YYYY-MM' format
    start_date = f"{month}-01"
    end_date = f"{month}-{calendar.monthrange(int(month[:4]), int(month[5:]))[1]}"

    sql_command = f"""
    DELETE FROM daily
    WHERE market = '{market}' AND trading_pair = '{trading_pair}' AND date >= '{start_date}' AND date <= '{end_date}';
    """
    print(f"Attempting to delete from DB: Market: {market}, Trading Pair: {trading_pair}, Month: {month}")
    run_sql_command(sql_command)
    print(colored(f"Deleted from DAILY DB ALL MARKETS IN: Market: {market}, Trading Pair: {trading_pair}, Month: {month}", 'yellow'))


# Modify the insert function to accept an additional boolean parameter
def insert_new_file_record_monthly(market, trading_pair, date_str, first_csv):
    formatted_trading_pair = trading_pair.replace('/', '')
    sql_command = f"""
    INSERT INTO monthly (market, trading_pair, date, normalized, inserted_to_psql, is_delisted, first_csv)
    VALUES ('{market}', '{formatted_trading_pair}', '{date_str}', '0', '0', '0', {int(first_csv)});
    """
    run_sql_command(sql_command)
    print(colored(f"Inserted into DB: {market}, {formatted_trading_pair}, {date_str}, first_csv: {first_csv}", 'cyan'))


def scan_daily_storage_for_csv_files(STORAGE_PATH_DAILY):
    print("Scanning daily storage for CSV files...")
    files_info = []  # Holds information about the files
    for root, dirs, files in os.walk(STORAGE_PATH_DAILY):
        for file in files:
            if file.endswith('.csv'):
                parts = file.replace('-trades', '').split('-')
                symbol = parts[0]
                date_str = '-'.join(parts[1:])
                date_obj = datetime.strptime(date_str.replace('.csv', ''), '%Y-%m-%d')
                rel_dir = os.path.relpath(root, STORAGE_PATH_DAILY)
                market = rel_dir.split(os.path.sep)[0]
                files_info.append((market, symbol, date_str.replace('.csv', '')))
    print(f"Found {len(files_info)} files in daily storage.")
    return files_info


# Function to scan the storage path and create a list of files
def scan_monthly_storage_for_csv_files(STORAGE_PATH_MONTHLY):
    files_info = []  # Holds information about the files
    for root, dirs, files in os.walk(STORAGE_PATH_MONTHLY):
        for file in files:
            if file.endswith('.csv'):
                parts = file.replace('-trades', '').split('-')
                symbol = parts[0]
                date_str = '-'.join(parts[1:])
                date_obj = datetime.strptime(date_str.replace('.csv', ''), '%Y-%m')
                rel_dir = os.path.relpath(root, STORAGE_PATH_MONTHLY)
                market = rel_dir.split(os.path.sep)[0]
                files_info.append((market, symbol, date_str.replace('.csv', '')))
    return files_info


#-----------------------------------------------------------------------------------------------------------#


# Logic for determining the oldest csv file
def first_csv():
    sql_command = "SELECT market, trading_pair, date, first_csv FROM daily WHERE first_csv = TRUE;"
    first_csv_records = run_sql_command(sql_command)
    if first_csv_records:
        records = [record.split("\t") for record in first_csv_records.split("\n") if len(record.split("\t")) == 4]
        return set([(r[0], r[1], r[2][:7]) for r in records])  # (market, symbol, month)
    else:
        print("No first_csv records found.")
        return set()


# Function for deleting data off of the HDD
def delete_csv_from_HDD(market, symbol, month):
    daily_files_pattern = os.path.join(STORAGE_PATH_DAILY, market, symbol, f"{symbol}-trades-{month}-*.csv")
    for f in glob.glob(daily_files_pattern):
        os.remove(f)


def update_first_csv_in_db(market, trading_pair, month):
    # SQL command to update the first_csv flag for the given month
    sql_command = f"""
    UPDATE monthly
    SET first_csv = 1
    WHERE market = '{market}' AND trading_pair = '{trading_pair}' AND date LIKE '{month}%';
    """
    run_sql_command(sql_command)
    print(colored(f"Updated first_csv in DB for {market}, {trading_pair}, month: {month}", 'green'))


def count_lines_in_file(file_path):
    """Counts the number of lines in a given file."""
    with open(file_path, 'r') as file:
        return sum(1 for _ in file)


#-----------------------------------------------------------------------------------------------------------#


# Function to identify complete months or months with first_csv file
def identify_complete_months(files_info, first_csv_months):
    print("Identifying complete months...")
    trading_pair_dates = {}
    for market, symbol, date_str in files_info:
        key = (market, symbol)
        if key not in trading_pair_dates:
            trading_pair_dates[key] = []
        trading_pair_dates[key].append(date_str)

    complete_months = {}
    for key, dates in trading_pair_dates.items():
        print(f"Checking complete months for {key} with {len(dates)} dates.")
        dates = set(dates)
        months = {date[:7] for date in dates}  # Extract YYYY-MM
        for month in months:
            complete_month = all(f"{month}-{day:02d}" in dates for day in range(1, calendar.monthrange(int(month[:4]), int(month[5:]))[1] + 1))
            if complete_month or (key[0], key[1], month) in first_csv_months:
                if key not in complete_months:
                    complete_months[key] = []
                complete_months[key].append(month)

    print(f"Identified complete months for {len(complete_months.keys())} trading pairs.")
    return complete_months


#-----------------------------------------------------------------------------------------------------------#


def aggregate_to_monthly(market, symbol, month, first_csv_months, max_retries=3):
    print(f"Aggregating data to monthly for market: {market}, symbol: {symbol}, month: {month}")

    # Directory for monthly aggregated files
    monthly_dir_path = os.path.join(STORAGE_PATH_MONTHLY, market, symbol)
    os.makedirs(monthly_dir_path, exist_ok=True)

    daily_files_pattern = os.path.join(STORAGE_PATH_DAILY, market, symbol, f"{symbol}-trades-{month}-*.csv")
    monthly_file_path = os.path.join(monthly_dir_path, f"{symbol}-trades-{month}.csv")

    # Determine if this month should be marked as first_csv
    first_csv_month = (market, symbol, month) in first_csv_months

    attempt = 0
    while attempt < max_retries:
        try:
            total_daily_lines = 0
            with open(monthly_file_path, 'w') as monthly_file:
                for daily_file in glob.glob(daily_files_pattern):
                    line_count = count_lines_in_file(daily_file)
                    total_daily_lines += line_count

                    with open(daily_file, 'r') as df:
                        for line in df:
                            monthly_file.write(line)

            # Verify if all lines were written to the monthly file
            total_monthly_lines = count_lines_in_file(monthly_file_path)
            if total_daily_lines != total_monthly_lines:
                raise ValueError("Line count mismatch: daily total vs. monthly total")

            print(f"Successfully aggregated monthly data for {market}-{symbol} for month: {month}")

            # Insert record into the database for the monthly file, setting the first_csv flag if necessary
            insert_new_file_record_monthly(market, symbol, month, first_csv_month)
            print(f"Inserted monthly data for {market}-{symbol} for month: {month}, first_csv: {first_csv_month}")

            # Delete daily CSV files from HDD and database only if verification is successful
            delete_csv_from_HDD(market, symbol, month)
            delete_record_from_db_daily(market, symbol, month)
            return

        except ValueError as e:
            print(colored(f"Error in aggregation attempt {attempt + 1}: {e}", 'red'))
            os.remove(monthly_file_path)  # Delete the incorrect monthly file
            attempt += 1

    print(f"Failed to aggregate data after {max_retries} attempts.")


#-----------------------------------------------------------------------------------------------------------#


# Process STORAGE_PATH and package daily csvs into monthly csvs then store
def process_symbols(market_type, executor):
    print(f"Processing symbols for market type: {market_type}")
    daily_files_info = scan_daily_storage_for_csv_files(STORAGE_PATH_DAILY)

    # Get months with first_csv flag
    first_csv_months = first_csv()
    print("First CSV months:", first_csv_months)

    # Identify complete months or months with first_csv file
    complete_months = identify_complete_months(daily_files_info, first_csv_months)

    # Aggregate daily files into monthly files for each complete month
    for (market, symbol), months in complete_months.items():
        for month in months:
            aggregate_to_monthly(market, symbol, month, first_csv_months)




#-----------------------------------------------------------------------------------------------------------#


# Main logic
if __name__ == "__main__":
    print("Starting main process...")
    # Get the list of downloaded files from the storage
    files_in_storage_monthly = scan_monthly_storage_for_csv_files(STORAGE_PATH_MONTHLY)

    # Get the list of downloaded records from the database
    downloaded_records_db_monthly = get_downloaded_records_from_db_monthly()


    # Delete records from the database that are not present in storage
    for record in downloaded_records_db_monthly:
        if (record[0], record[1], record[2]) not in [(f[0], f[1], f[2]) for f in files_in_storage_monthly]:
            delete_record_from_db_monthly(*record)


    # Insert new records for files present in storage but not in the database
    for file_info in files_in_storage_monthly:
        if (file_info[0], file_info[1], file_info[2]) not in [(r[0], r[1], r[2]) for r in downloaded_records_db_monthly]:
            insert_new_file_record_monthly(*file_info, False)


    # Use threading to download files
    with ThreadPoolExecutor(max_workers=1) as executor:
        for market_type in MARKET_TYPES.keys():
            process_symbols(market_type, executor)

    print("Main process completed.")
