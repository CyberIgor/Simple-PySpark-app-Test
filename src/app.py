"""
This script reads two CSV files using PySpark, filters rows from the first DataFrame based on specified values, 
performs an inner join with the second DataFrame, renames columns, and saves the resulting DataFrame to a CSV file.

Usage:
    python app.py --df1_path path_to_clients_file --df2_path path_to_transactions_file --values_to_filter values_to_filter

Arguments:
    --df1_path (str): Path to the CSV file containing client data.
    --df2_path (str): Path to the CSV file containing transaction data.
    --values_to_filter (str): Comma-separated values used for filtering the client DataFrame.

Output:
    - The resulting DataFrame after joining and renaming columns is displayed.
    - The number of rows in the resulting DataFrame is printed.
    - A log file ('events.log') is generated to record script events.

Note:
    This script assumes that the input CSV files have headers and infers the schema.

Dependencies:
    - PySpark
    - argparse
    - logging

Example:
    python app.py --df1_path clients.csv --df2_path transactions.csv --values_to_filter USA,Canada
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging
import argparse

logging.basicConfig(filename='events.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s: %(message)s\n', filemode='a')

# Create an ArgumentParser object:
parser = argparse.ArgumentParser(description="Parser for the required arguments")

parser.add_argument('--df1_path', type=str, help='Path to clients file')
parser.add_argument('--df2_path', type=str, help='Path to transactions file')
parser.add_argument('--values_to_filter', type=str, help='Values to filter')

# Parse the command-line arguments:
args = parser.parse_args()
logging.info("Three arguments were received from the command line.")

# Starting Spark session:
spark = SparkSession.builder.appName("ReadCSV").getOrCreate()
logging.info("Spark session has begun.")

# Creating custom function for filtering dataframe:
def filtering(df: DataFrame, values_to_filter: str, filtering_field="country"):
    """
    Filter PySpark DataFrame using specified values of a filtering field.
    
    Parameters:
        df (DataFrame): The input DataFrame.
        
        values_to_filter (str): A list of values to filter by (must be passed as a string).
        If more than one is required, than values must be separated by a comma and without spaces.
        Exception - values which themselves contain commas (like "United Kingdom" when filtering by country name).
        
        filtering_field (str): a field name which values will be used to filter by.

    Returns:
        DataFrame: A DataFrame filterd by specified values of a given column.
    """

    return df.filter(df[filtering_field].isin(*values_to_filter.split(",")))

# Creating custom function for renaming columns:
def rename_columns(df: DataFrame, column_mapping: dict):
    """
    Rename columns in a PySpark DataFrame.

    Parameters:
        df (DataFrame): The input DataFrame.
        column_mapping (dict): A dictionary where keys are old column names and values are new column names.

    Returns:
        DataFrame: A DataFrame with the specified column renames.
    """

    for old_col, new_col in column_mapping.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df

# Access the argument values:
df1_path = args.df1_path
df2_path = args.df2_path
values_to_filter = args.values_to_filter

# Reading csv-files and omitting redundant fields:
df1 = spark.read.csv(df1_path, header=True, inferSchema=True).drop(*["first_name", "last_name"])
df2 = spark.read.csv(df2_path, header=True, inferSchema=True).drop("cc_n")
logging.info("Two PySpark dataframes were created out of CSV-files. Sensetive information has been omitted.")

# Filtering clients table and joining it with transactions table:
output_df = filtering(df1, values_to_filter).join(df2, on="id", how="inner")
logging.info("Client records were filtered - and two dataframes were joined using 'id' field as a primary key.")

column_mapping = {
    "id": "client_identifier",
    "btc_a": "bitcoin_address",
    "cc_t": "credit_card_type"
}

# Renaming columns:
output_df = rename_columns(output_df, column_mapping)
logging.info("Output dataframe columns renamed.")

# Displaying the number of records in the output dataframe:
output_df.show()
print(f"\nNumber of rows in output dataframe: {output_df.count()}\n")

# Setting up the correct dataframe schema for further testing:
columns = ["client_identifier", "email", "country", "bitcoin_address", "credit_card_type"]
data_types = [IntegerType(), StringType(), StringType(), StringType(), StringType()]
expected_schema = StructType([StructField(col, data_type, True) for col, data_type in zip(columns, data_types)])

# Checking if the output dataframe matches the expected schema, logging events and storing output dataframe into `client_data` folder:
if output_df.schema == expected_schema:
    logging.info("Output DataFrame completely matches the expected schema.")
    output_df.write.csv("client_data", header=True, mode="overwrite")
    logging.info("New data was saved to 'client_data' folder.")
else:
    if len(output_df.schema) == len(expected_schema):
        if all(output_df.schema[i].dataType == expected_schema[i].dataType for i in range(len(expected_schema))):
            logging.warning("Output DataFrame matches the expected schema by the number of fields and respective data types but doesn't match by column names.")
            output_df.write.csv("client_data", header=True, mode="overwrite")
            logging.info("New data was saved to 'client_data' folder.")
        else:
            logging.error("Output DataFrame matches the expected schema by the number of fields but doesn't match by data types.")
            logging.error("No new data was saved to 'client_data' folder. The issue with data types must be fixed.")
    else:
        logging.error("Output DataFrame doesn't match the expected schema by the number of fields.")
        logging.error("No new data was saved to 'client_data' folder. The issue with number of columns must be fixed.")
