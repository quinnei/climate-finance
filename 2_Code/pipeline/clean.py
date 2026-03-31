"""Clean and recode tabular datasets."""

import re
import pandas as pd


def _standardize_column_name(column):
    """Convert a column label into lowercase words joined by underscores."""
    column = str(column).lower()
    column = re.sub(r'[\(\)\-]', '', column)
    column = re.sub(r'\s+', '_', column)
    return column


def reorder_columns(data, columns_to_rearrange):
    """Move selected columns to the front while preserving the remaining order."""
    return data[
        columns_to_rearrange +
        [column for column in data.columns if column not in columns_to_rearrange]
    ]


# --------------------------------------------------
# A. RENAME COLUMNS
# --------------------------------------------------

def rename_columns(data, coding_instructions, standardize_column_names = False):
    """
    Rename columns using a mapping and optionally standardize all column labels.

    Parameters
    ----------
    data : pd.DataFrame
        Input dataframe.
    coding_instructions : dict
        Dictionary that maps original column names to new names.
    standardize_column_names : bool, default False
        If True, all resulting column labels are normalized to a simple,
        lowercase underscore format.

    Returns
    -------
    pd.DataFrame
        Dataframe with renamed columns.
    """
    data = data.rename(columns = coding_instructions)

    if standardize_column_names:
        data.columns = [_standardize_column_name(column) for column in data.columns]

    return data



# --------------------------------------------------
# B. RECODE VALUES OF A GIVEN COLUMN
# --------------------------------------------------

# Assign new labels to a given column.

def recode_values(data, column, coding_instructions, based_on = None):
    """Recode values of a given column using `coding_instructions`.

    Parameters
    ----------
    data : pd.DataFrame
        Input dataframe.
    column : str
        The column that needs to be updated.
    coding_instructions : dict
        Dictionary that maps original values (key) to recoded values (value).
    based_on : str, default None
        A reference column used to determine which rows should be recoded.
        - If omitted: values of the `column` variable are used to determine which rows to update.
        - If provided: values of the 'based_on' column are used to determine which rows to update.

    Returns
    -------
    pd.DataFrame
        The same dataframe with updated values in `column`.
        Values not included in `coding_instructions` are kept as-is.
        Missing values are left unchanged.
    """
    if based_on is None:
        data[column] = data[column].replace(coding_instructions)
    else:
        replacement = data[based_on].map(coding_instructions)
        data[column] = replacement.combine_first(data[column])

    return data

def fill_missing_values(data, column, recoded_value, condition):
    """Fill missing values in one column where a condition is met.

    This helper is intended for targeted filling rules such as:
    1. fill only rows that are currently missing in the target column, and
    2. fill only the subset of those missing rows that satisfy `condition`.

    Parameters
    ----------
    data : pd.DataFrame
        Input dataframe.
    column : str
        Column in which missing values should be filled.
    recoded_value : scalar or pd.Series
        Value to insert into missing cells that satisfy `condition`.
    condition : bool or pd.Series
        Boolean mask that marks which missing rows should be updated.
    """
    should_recode = data[column].isna() & condition
    data.loc[should_recode, column] = recoded_value
    return data


def remove_prefix(data, column):
    """Remove numeric prefix from all unique values in a column.

    Parameters
    ----------
    column : str
        Column that needs prefix removal.

    Returns
    -------
    pd.DataFrame
        DataFrame with the recoded column.
    """
    # Extract unique values from the given column.
    # Ultimately, it will be used to construct the codebook,
    # with original labels (as key) - new labels (as value). 
    labels = data[column].unique().tolist()

    def text_without_prefix(label):
        """Return the portion of a single label after the first ``'. '``.

        Parameters
        ----------
        label : object
            A value from the given column (a single element, not a list of values). 
            Could be a string or `NaN`.
        """
        # Preserve the missing values, instead of converting them to string form 'nan'
        if pd.isna(label):
            return label
        # If the label contains a dot (.) followed by a whitespace ( )
        # Split it up into two parts: text that comes before the dot-space sequence and after.
        if '. ' in str(label):
            # Extract what comes after the prefix (the descriptive part of the label)
            return str(label).split('. ', 1)[1]
        # Otherwise, leave the label unchanged
        return label
    
    coding_instructions = {original_label: text_without_prefix(original_label) for original_label in labels}
    return recode_values(data, column, coding_instructions)


# --------------------------------------------------
# C. MERGE DATASETS
# --------------------------------------------------

# MERGE UP TO FOUR DATASETS SEQUENTIALLY 
# (PIPING ONE OUTPUT AFTER ANOTHER), BASED ON A COMMON KEY

def merge_data(
    data_1,
    data_2,
    key,
    data_3 = None,
    data_4 = None,
    join_type = None,
    columns_to_remove = None,
    columns_to_rename = None,
    columns_to_rearrange = None):
    """Merge up to four datasets sequentially using the same join keys.

    The sequence is:
    1. start with `data_1`,
    2. merge in `data_2`,
    3. optionally merge in `data_3` and `data_4`,
    4. optionally remove, rename, and reorder columns.

    Parameters
    ----------
    data_1 : pd.DataFrame
        Base dataframe to which the later dataframes will be joined.
    data_2 : pd.DataFrame
        First dataframe to join onto `data_1`.
    key : str or list
        Column(s) used to merge datasets
    data_3, data_4 : pd.DataFrame, optional
        Additional dataframes to merge in sequence after `data_2`.
    join_type : list[str], optional
        A list of join methods for each merge step.
        Length must equal the number of merges performed (the number of datasets - 1).
        Defaults to 'left' for all merges.
        Type of merge ('left', 'right', 'inner', 'outer')
        Example:
        - 2 datasets total  -> ['left']
        - 3 datasets total  -> ['inner', 'left']
        - 4 datasets total  -> ['inner', 'left', 'left']
    columns_to_remove : str or list[str], optional
        Columns to drop after merge
    columns_to_rename : dict, optional
        Mapping of columns to rename after merge        
    columns_to_rearrange : list[str], optional
        Columns to place first, in the specified order

    Returns
    -------
    pd.DataFrame
        Merged dataframe with optional transformation on columns.
    """

    # 1. Build the ordered list of dataframes that will be merged into the
    # starting dataframe.
    datasets_to_merge = [data_2, data_3, data_4]
    datasets_to_merge = [dataset for dataset in datasets_to_merge if dataset is not None]

    # 2. Default to left joins unless the caller explicitly requests otherwise.
    if join_type is None:
        join_type = ['left'] * len(datasets_to_merge)

    # 3. Validate that one join type has been provided for each merge step.
    if len(join_type) != len(datasets_to_merge):
        raise ValueError("Length of join_type must match number of merges")

    # 4. Start from the primary dataframe and merge each later dataframe in turn.
    merged_data = data_1

    for dataset_to_merge, join_method in zip(datasets_to_merge, join_type):
        merged_data = pd.merge(
            merged_data,
            dataset_to_merge,
            on = key,
            how = join_method,
        )

    # 5. Apply optional post-merge cleanup in a predictable order:
    # remove columns, then rename columns, then reorder columns.
    if columns_to_remove is not None:
        merged_data = merged_data.drop(columns = columns_to_remove)

    if columns_to_rename is not None:
        merged_data = merged_data.rename(columns = columns_to_rename)

    if columns_to_rearrange is not None:
        merged_data = reorder_columns(merged_data, columns_to_rearrange)

    return merged_data


def calculate_commitment_per_capita(
        data,
        years = None,
        commitment_prefix = 'USD',
        population_prefix = 'POP',
        total_output = 'total_commitment',
        annual_output_prefix = 'CRDF_PER_CAPITA',
        aggregate_output = 'commitment_per_capita',
        commitment_scale = 1000,
        columns_to_rearrange = None):
    """Calculate yearly and aggregate (2015-2023 combined) commitment indicators.

    The outputs are:
    1. `total_output`: the sum of all yearly commitment columns after scaling
       them from thousand USD into unit USD.
    2. `annual_output_prefix_<year>`: each year's commitment per capita, such as
       `CRDF_PER_CAPITA_2015`.
    3. `aggregate_output`: the sum of the yearly commitment-per-capita values
       across the full study period.
    """
    if years is None:
        years = range(2015, 2024)

    # 1. Compute the aggregate commitment (2015-2023 combined) in unit USD.
    data[total_output] = (
        data[[f'{commitment_prefix}_{year}' for year in years]].sum(axis = 1) * commitment_scale
    )

    # 2. Compute one annual commitment-per-capita column for each year.
    for year in years:
        data[f'{annual_output_prefix}_{year}'] = (
            data[f'{commitment_prefix}_{year}'] * commitment_scale
        ) / data[f'{population_prefix}_{year}']

    # 3. Sum the annual commitment-per-capita columns over the study period.
    data[aggregate_output] = data[
        [f'{annual_output_prefix}_{year}' for year in years]
    ].sum(axis = 1)

    # 4. Drop the wide yearly commitment and population inputs once the derived
    # indicators have been created.
    data = data.drop(
        columns = [
            column for column in data.columns
            if column.startswith((f'{commitment_prefix}_', f'{population_prefix}_'))
        ]
    )

    if columns_to_rearrange is None:
        columns_to_rearrange = [
            'recipient', 'region', 'SIDS', 'income',
            total_output, aggregate_output
        ]

    # 5. Move the most important identifier and summary columns to the front.
    data = reorder_columns(data, columns_to_rearrange)

    return data
