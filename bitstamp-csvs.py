import subprocess

def run_sql_command(sql_command, database_name=""):
    cmd = [
        "mysql",
        "--login-path=client",
        "-e",
        sql_command,
        database_name
    ]
    subprocess.run(cmd, check=True)

# SQL commands to create the database and tables
database_query = "CREATE DATABASE IF NOT EXISTS bitstamp_csvs;"

hourly_table_query = """
CREATE TABLE IF NOT EXISTS bitstamp_csvs.hourly (
    market VARCHAR(10) NULL,
    trading_pair VARCHAR(25) NULL,
    date VARCHAR(20) NULL,
    normalized BOOLEAN NULL,
    inserted_to_psql BOOLEAN NULL,
    is_delisted BOOLEAN NULL,
    first_csv BOOLEAN NULL,
    UNIQUE INDEX idx_market_pair_date (market, trading_pair, date)
);
"""

daily_table_query = """
CREATE TABLE IF NOT EXISTS bitstamp_csvs.daily (
    market VARCHAR(10) NULL,
    trading_pair VARCHAR(25) NULL,
    date VARCHAR(20) NULL,
    normalized BOOLEAN NULL,
    inserted_to_psql BOOLEAN NULL,
    is_delisted BOOLEAN NULL,
    first_csv BOOLEAN NULL,
    UNIQUE INDEX idx_market_pair_date (market, trading_pair, date)
);
"""

monthly_table_query = """
CREATE TABLE IF NOT EXISTS bitstamp_csvs.monthly (
    market VARCHAR(10) NULL,
    trading_pair VARCHAR(25) NULL,
    date VARCHAR(20) NULL,
    normalized BOOLEAN NULL,
    inserted_to_psql BOOLEAN NULL,
    is_delisted BOOLEAN NULL,
    first_csv BOOLEAN NULL,
    UNIQUE INDEX idx_market_pair_date (market, trading_pair, date)
);
"""

# Run the SQL commands using the run_sql_command function
run_sql_command(database_query)
run_sql_command(hourly_table_query)
run_sql_command(daily_table_query)
run_sql_command(monthly_table_query)
