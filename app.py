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

# Starting Spark session:
spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

# Creating custom function for filtering dataframe:
def filtering(df: DataFrame, values_to_filter: list, filtering_field="country"):
    """
    Filter PySpark DataFrame using specified values of a filtering field.

    Parameters:
        df (DataFrame): The input DataFrame.
        values_to_filter (list): A list of values to filter by.

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

# Filtering clients table and joining it with transactions table:
output_df = filtering(df1, values_to_filter).join(df2, on="id", how="inner")

column_mapping = {
    "id": "client_identifier",
    "btc_a": "bitcoin_address",
    "cc_t": "credit_card_type"
}

# Renaming columns:
output_df = rename_columns(output_df, column_mapping)

# Displaying the number of records in the output dataframe:
output_df.show()
print(f"\nNumber of rows in output dataframe: {output_df.count()}\n")

# Setting up the correct dataframe schema for further testing:
columns = ["client_identifier", "email", "country", "bitcoin_address", "credit_card_type"]
data_types = [IntegerType(), StringType(), StringType(), StringType(), StringType()]
expected_schema = StructType([StructField(col, data_type, True) for col, data_type in zip(columns, data_types)])

# Checking if the output dataframe matches the expected schema and logging events:
if output_df.schema == expected_schema:
    logging.info("Output DataFrame completely matches the expected schema.")
else:
    if len(output_df.schema) == len(expected_schema):
        if all(output_df.schema[i].dataType == expected_schema[i].dataType for i in range(len(expected_schema))):
            logging.warning("Output DataFrame matches the expected schema by the number of fields and respective data types but doesn't match by column names.")
        else:
            message = "Output DataFrame matches the expected schema by the number of fields but doesn't match by data types."
            raise Exception(message)
    else:
        message = "Output DataFrame doesn't match the expected schema by the number of fields."
        raise Exception(message)

# Storing output dataframe into `client_data` folder:
output_df.write.csv("client_data", header=True, mode="overwrite")
