"""Inspect datasets during loading, cleaning, and saving."""

import pandas as pd


def _get_dataframe_name(data):
    """Return the dataframe label stored in `data.attrs['dataframe_name']`."""
    dataframe_name = data.attrs.get('dataframe_name')
    if not isinstance(dataframe_name, str) or not dataframe_name:
        raise ValueError("data.attrs['dataframe_name'] must be provided as a non-empty string.")
    return dataframe_name


def inspect_data(data, csv_file_name = None, save_missing_rows = False):
    """
    Print the datatype and missing-value summary for the given dataframe.
    """
    dataframe_name = _get_dataframe_name(data)

    inspect_datatype(data)
    inspect_missing_values(
        data,
        csv_file_name = csv_file_name or f'{dataframe_name}_rows_with_NA',
        save_missing_rows = save_missing_rows,
    )



# --------------------------------------------------
# A. INSPECT THE DIMENSIONS & STRUCTURE OF THE DATA
# --------------------------------------------------

def inspect_datatype(data):
    """
    Print all 1) column names in the dataset as well as their 2) data types.
    """
    dataframe_name = _get_dataframe_name(data)
    print(f"\n[{dataframe_name}] dataset - Column data types:")
    for column, datatype in data.dtypes.items():
        print(f" - {column}: {datatype}")



# --------------------------------------------------
# B. CHECK FOR MISSING VALUES
# --------------------------------------------------

def inspect_missing_values(
        data,
        csv_file_name = "rows_with_NA",
        save_missing_rows = False):
    """
    1) Identify variables with missing values; and
    2) Optionally save rows with at least one missing value as a test CSV.
    """

    dataframe_name = _get_dataframe_name(data)

    # Count the number of missing values in each column
    number_of_NAs = data.isna().sum()
    # Remove columns that do not have any missing values
    columns_with_NAs = number_of_NAs[number_of_NAs > 0]
    
    # Display information about the dimensionality of the dataset.
    print(f"\n[{dataframe_name}] dataset - Dimensions and missing values:")
    print("------------------------------------------------")       
    print(f"Total number of rows: {len(data):,}")
    print(f"Total number of columns: {len(data.columns):,}")
    print("------------------------------------------------")       
    
    # If there are no missing values, print a message
    if columns_with_NAs.empty:
        print("No missing values detected in any column.")
        print("================================================")
    # Otherwise, display the list of column names and count of their missing values
    else:
        print("Columns with missing values and their counts:")
        for column, count in columns_with_NAs.items():
            print(f" - {column}: {count:,}")
        if save_missing_rows:
            from . import access
            missing_rows = data[data.isna().any(axis = 1)]
            access.save_to_csv(missing_rows, csv_file_name, for_testing = True)
        print("================================================")          

# --------------------------------------------------
# C. EXAMINE A SUBSET OF COLUMNS & ROWS
# REDUCE THE DIMENSIONS OF THE DATASET, FOR EASE OF EXPLORATION & COMPUTATIONAL EFFICIENCY
# PART 1. REMOVE ROWS ONLY
# --------------------------------------------------

### Filter rows based on the range of values in a given column.
### It works with both float & integer datatypes. 

def filter_rows_by_range(
        data, column,
        lower_bound = None, upper_bound = None,
        lower_inclusive = True, upper_inclusive = True):
    """Filter rows based on a given range.
    * If it's an integer: can use both filter_rows() & filter_by_numeric_range().
    * It it's a decimal/float: it only works with filter_by_numeric_range().

    When you provide:
    * Both the 1) lower_bound & 2) upper_bound: all inclusive (e.g. 1 <= x <= 5).
    * Only the lower_bound: Don't forget to specify `lower_inclusive = True/False` (>= or >).
    * Only the upper_bound: Don't forget to specify `upper_inclusive = True/False` (<= or <).
    * Only the 1) data & 2) column: returns the original dataframe.

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame to filter.
    column : str
        Name of the column based on which you will filter the rows.
    lower_bound : numeric or None, default None
        The lower bounds of a given range (Optional; Can be omitted).
    upper_bound : numeric or None, default None
        The upper bounds of a given range (Optional; Can be omitted).
    lower_inclusive : bool, default True
        When the lower_bound is provided, always use `>=` as default.
        If lower_inclusive is set to False, use `>`.
    upper_inclusive : bool, default True
        When the upper_bound is provided, always use `<=` as default.
        If upper_inclusive is set to False, use `<`.

    Returns
    -------
    pd.DataFrame
        Filtered rows matching the specified condition(s).
    """
    # Initially, mark every row as True (i.e. keep all rows)
    keep = pd.Series(True, index = data.index)

    # Check if the lower bound has been provided.
    # If so, and if lower_inclusive is set to True, use `>=`.
    # If so, and if lower_inclusive is set to False, use `>`.
    # Update the existing filter, which now consists of 'True's & False's.
    if lower_bound is not None:
        keep &= (data[column] >= lower_bound) if lower_inclusive else (data[column] > lower_bound)

    # Check if the upper bound has been provided.
    # If so, and if upper_inclusive is set to True, use `<=`.
    # If so, and if upper_inclusive is set to False, use `<`.
    # Update the existing filter according to the upper bound and its inclusivity/exclusivity.
    if upper_bound is not None:
        keep &= (data[column] <= upper_bound) if upper_inclusive else (data[column] < upper_bound)

    # Return only the rows where keep == True
    # i.e. rows that satisfy the specified condition(s)
    return data[keep]
