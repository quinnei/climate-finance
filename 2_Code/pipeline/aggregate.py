"""Aggregate and reshape multi-year datasets."""

import pandas as pd


def aggregate_by_year(
        data,
        value_column,
        group_columns = None,
        year_column = 'year',
        aggfunc = 'sum'):
    """
    Aggregate a value by year, optionally within one or more grouping columns.

    Examples
    --------
    - Total commitment by year
    - Commitment by provider and year
    - Project counts by recipient and year
    """
    if group_columns is None:
        group_columns = []

    grouping_keys = [*group_columns, year_column]
    aggregated = (
        data
        .groupby(grouping_keys, as_index = False)[value_column]
        .agg(aggfunc)
        .sort_values(grouping_keys)
        .reset_index(drop = True)
    )
    return aggregated


def aggregate_over_entire_timespan(
        data,
        value_column,
        group_columns,
        output_column = None,
        aggfunc = 'sum',
        ascending = False):
    """
    Aggregate a value across the entire time period covered by the study/research.
    """
    if output_column is None:
        output_column = value_column

    aggregated = (
        data
        .groupby(group_columns, as_index = False)[value_column]
        .agg(aggfunc)
        .rename(columns = {value_column: output_column})
        .sort_values(output_column, ascending = ascending)
        .reset_index(drop = True)
    )
    return aggregated


def add_pct_within_group(data, value_column, denominator_columns, output_column = 'percentage'):
    """
    Add a within-group percentage to an already aggregated dataframe.

    Parameters
    ----------
    data : pd.DataFrame
        Dataframe that already contains an aggregated value column.
    value_column : str
        Column holding the numerator values.
    denominator_columns : list[str]
        Columns that define the denominator group.
        Example:
        - ['year'] gives each category's share of the annual total.
        - ['region', 'year'] gives each category's share within each region-year.
    output_column : str, default 'percentage'
        Name of the resulting percentage column.
    """
    data = data.copy()
    totals = data.groupby(denominator_columns)[value_column].transform('sum')
    data[output_column] = (data[value_column] / totals).where(totals.ne(0), 0) * 100
    return data


def rank_within_group(
        data,
        value_column,
        group_columns,
        rank_column = 'rank',
        ascending = False,
        method = 'first'):
    """
    Rank rows within each group after aggregation.
    """
    data = data.copy()
    data[rank_column] = (
        data
        .groupby(group_columns)[value_column]
        .rank(method = method, ascending = ascending)
        .astype(int)
    )
    return data


def convert_wide_yearly_data_to_long(
        data,
        prefix,
        id_columns,
        value_name,
        year_name = 'year'):
    """
    Convert year-specific columns into a long table with one row per year.
    """
    # 1. Find all columns that match the requested yearly prefix.
    year_columns = sorted(
        [column for column in data.columns if column.startswith(f'{prefix}_')],
        key = lambda column: int(column.rsplit('_', 1)[-1]),
    )

    # 2. Replace labels such as `USD_2015` with the numeric year `2015`.
    renamed_columns = {
        column: int(column.rsplit('_', 1)[-1])
        for column in year_columns
    }

    # 3. Reshape the wide year columns into a long year-by-year table.
    long_data = (
        data[id_columns + year_columns]
        .rename(columns = renamed_columns)
        .melt(
            id_vars = id_columns,
            var_name = year_name,
            value_name = value_name,
        )
        .sort_values(id_columns + [year_name])
        .reset_index(drop = True)
    )

    # 4. Store the year column as an integer for sorting and later analysis.
    long_data[year_name] = long_data[year_name].astype(int)
    return long_data
