from pyspark.sql import DataFrame
from logging import getLogger, Formatter, INFO
from logging.handlers import RotatingFileHandler

# Set up the logger:
logger = getLogger('my_app_logger')
logger.setLevel(INFO)

# Create a RotatingFileHandler with a maximum file size of 5 MB and keep 5 backup files:
log_file = 'events.log'
handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5)

# Create a formatter for logger:
formatter = Formatter('%(asctime)s - %(levelname)s: %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger:
logger.addHandler(handler)

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
