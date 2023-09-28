from pyspark.sql import DataFrame

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
