"""Build summary tables (pivot tables) from the final cleaned datasets."""

from . import access
from . import aggregate


SUMMARY_FOLDER = '1_Data/pivot_tables'


def _exclude_unearmarked_recipient_categories(data):
    """
    When country-level analysis is required, remove rows where
    finance was allocated at the regional-level or was unspecified
    (exclude only from the output table, but keep them in the final, master dataset).
    """
    exclusion_mask = (
        data['recipient'].astype(str).str.contains(
            r'regional|subregional|developing countries|unspecified',
            case = False,
            na = False,
        )
    )
    return data.loc[~exclusion_mask].copy()


def view_annual_total_commitment_table(data):
    """Aggregate the full dataset into one total commitment amount for each year."""
    return aggregate.aggregate_by_year(
        data,
        value_column = 'commitment',
    )


def view_provider_yearly_commitment_table(data):
    """Aggregate commitment by donor and year so the output can be filtered later."""
    return aggregate.aggregate_by_year(
        data,
        value_column = 'commitment',
        group_columns = ['provider_type', 'provider'],
    )


def view_provider_total_commitment_table(data):
    """Aggregate donor totals across the full analysis period."""
    grouped = aggregate.aggregate_over_entire_timespan(
        data,
        value_column = 'commitment',
        group_columns = ['provider_type', 'provider'],
        output_column = 'total_commitment',
    )
    return (
        grouped
        .sort_values(
            ['provider_type', 'total_commitment'],
            ascending = [True, False],
        )
        .reset_index(drop = True)
    )


def view_recipient_yearly_commitment_table(data):
    """Aggregate commitment by recipient and year, excluding non-country (regional) entities."""
    recipient_data = _exclude_unearmarked_recipient_categories(data)
    return aggregate.aggregate_by_year(
        recipient_data,
        value_column = 'commitment',
        group_columns = ['recipient', 'region', 'income'],
    )


def view_recipient_total_commitment_table(data):
    """Aggregate country recipient totals across the full analysis period."""
    recipient_data = _exclude_unearmarked_recipient_categories(data)
    grouped = aggregate.aggregate_over_entire_timespan(
        recipient_data,
        value_column = 'commitment',
        group_columns = ['recipient', 'region', 'income'],
        output_column = 'total_commitment',
    )
    return grouped.sort_values('total_commitment', ascending = False).reset_index(drop = True)


def view_region_yearly_commitment_table(data):
    """Aggregate commitment by recipient region and year."""
    return aggregate.aggregate_by_year(
        data,
        value_column = 'commitment',
        group_columns = ['region'],
    )


def view_region_concessionality_share_table(data):
    """
    Build a table that shows the share of concessional climate finance, by region.
    """
    grouped = aggregate.aggregate_by_year(
        data,
        value_column = 'commitment',
        group_columns = ['region', 'concessionality'],
    )
    return aggregate.add_pct_within_group(
        grouped,
        value_column = 'commitment',
        denominator_columns = ['region', 'year'],
    )


def view_top_donors_by_recipient_table(data, top_n = 10):
    """
    For each recipient, construct a ranking of its top donors
    (throughout the entire timespan).

    Each recipient will have up to `top_n` donor rows.
    """
    recipient_data = _exclude_unearmarked_recipient_categories(data)
    grouped = aggregate.aggregate_over_entire_timespan(
        recipient_data,
        value_column = 'commitment',
        group_columns = ['recipient', 'region', 'income', 'provider', 'provider_type'],
        output_column = 'total_commitment',
    )
    grouped = aggregate.rank_within_group(
        grouped,
        value_column = 'total_commitment',
        group_columns = ['recipient'],
        rank_column = 'donor_rank',
    )
    grouped = (
        grouped[grouped['donor_rank'] <= top_n]
        .rename(columns = {'donor_rank': 'rank'})
        .loc[:, ['recipient', 'region', 'income', 'rank', 'provider', 'provider_type', 'total_commitment']]
        .sort_values(['recipient', 'rank'], ascending = [True, True])
        .reset_index(drop = True)
    )
    return grouped


def view_top_recipients_by_provider_table(data, top_n = 10):
    """
    For each provider, construct a ranking of the top recipients of climate finance
    (throughout the entire timespan).
    """
    recipient_data = _exclude_unearmarked_recipient_categories(data)
    grouped = aggregate.aggregate_over_entire_timespan(
        recipient_data,
        value_column = 'commitment',
        group_columns = ['provider', 'provider_type', 'recipient', 'region', 'income'],
        output_column = 'total_commitment',
    )
    grouped = aggregate.rank_within_group(
        grouped,
        value_column = 'total_commitment',
        group_columns = ['provider'],
        rank_column = 'recipient_rank',
    )
    grouped = (
        grouped[grouped['recipient_rank'] <= top_n]
        .rename(columns = {'recipient_rank': 'rank'})
        .loc[:, ['provider', 'provider_type', 'rank', 'recipient', 'region', 'income', 'total_commitment']]
        .sort_values(['provider_type', 'provider', 'rank'], ascending = [True, True, True])
        .reset_index(drop = True)
    )
    return grouped


def view_recipient_per_capita_yearly_table(per_capita):
    """
    Convert wide yearly per-capita columns into a long recipient-year dataset
    (for tracing the yearly commitment per-capita values for each recipient).

    Each yearly per-capita value is kept together with the recipient's
    2015-2023 total commitment and 2015-2023 aggregate per-capita amount.
    This lets one table support both year-by-year analysis and full-period
    comparisons.
    """
    id_columns = ['recipient', 'region', 'income', 'SIDS']

    # 1. Reshape the yearly per-capita columns so each recipient has one row
    # per year.
    long_data = aggregate.convert_wide_yearly_data_to_long(
        per_capita,
        prefix = 'CRDF_PER_CAPITA',
        id_columns = id_columns,
        value_name = 'yearly_commitment_per_capita',
    )

    # 2. Keep the full-period totals beside the yearly values for comparison.
    totals = per_capita[['recipient', 'total_commitment', 'commitment_per_capita']]
    return long_data.merge(totals, on = 'recipient', how = 'left')


def build_analysis_tables(final_data, per_capita_data):
    """
    Return all project-specific analysis tables as a dictionary.

    Keeping them together in one dictionary makes it easy to save them, inspect
    them interactively, or selectively reuse only the views needed for a given
    research task.
    """
    return {
        'annual_total_commitment': view_annual_total_commitment_table(final_data),
        'provider_yearly_commitment': view_provider_yearly_commitment_table(final_data),
        'provider_total_commitment': view_provider_total_commitment_table(final_data),
        'recipient_yearly_commitment': view_recipient_yearly_commitment_table(final_data),
        'recipient_total_commitment': view_recipient_total_commitment_table(final_data),
        'region_yearly_commitment': view_region_yearly_commitment_table(final_data),
        'region_concessionality_share': view_region_concessionality_share_table(final_data),
        'top_donors_by_recipient': view_top_donors_by_recipient_table(final_data),
        'top_recipients_by_provider': view_top_recipients_by_provider_table(final_data),
        'recipient_per_capita_yearly': view_recipient_per_capita_yearly_table(per_capita_data),
    }


def save_analysis_tables(tables, folder_name = SUMMARY_FOLDER):
    """
    Save all summary-ready tables to a dedicated subfolder.

    They are intentionally not labeled '[FINAL]'. The final project datasets are
    still the two canonical CSV files in '1_Data/final'. These derivative
    tables are saved as Excel files so the first row can carry filter dropdowns
    for interactive analysis.
    """
    saved_paths = {}

    for table_name, data in tables.items():
        saved_paths[table_name] = access.save_to_excel(
            data,
            file_name = table_name,
            folder_name = folder_name,
            prefix = '[SUMMARY]',
            dataframe_name = table_name,
        )

    return saved_paths
