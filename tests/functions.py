def filtering(df: DataFrame, values_to_filter: str, filtering_field="country"):
    return df.filter(df[filtering_field].isin(*values_to_filter.split(",")))

# Creating custom function for renaming columns:
def rename_columns(df: DataFrame, column_mapping: dict):
    for old_col, new_col in column_mapping.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df
