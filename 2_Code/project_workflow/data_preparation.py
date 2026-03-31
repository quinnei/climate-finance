"""Build the cleaned datasets and codebook inputs for this project."""

import country_converter as coco

import pipeline.access as access
import pipeline.clean as clean
import pipeline.explore as explore
import pipeline.summarize as summarize
from .config_main import (
    CONCESSIONALITY_RECODING,
    COUNTRIES_TO_REASSIGN_TO_MIDDLE_EAST,
    CRDF_COLUMN_RENAMES,
    END_YEAR,
    FILE_INFO,
    MVI_COLUMN_RENAMES,
    POPULATION_COLUMN_RENAMES,
    PROVIDER_RECODING,
    PROVIDER_TYPE_RECODING,
    RECIPIENT_RECODING,
    REGION_RECODING,
    RIO_MARKER_COLUMNS,
    RIO_MARKER_RECODING,
    SIDS_COLUMN_RENAMES,
    SIDS_MANUAL_RECODING,
    START_YEAR,
    YEARS,
)


def load_input_data():
    """Load every raw input dataset listed in `config_main.FILE_INFO`."""
    return {
        dataset_name: access.load_table(
            inspect_name = dataset_name,
            **file_info,
        )
        for dataset_name, file_info in FILE_INFO.items()
    }


def prepare_crdf_dataset(raw_crdf, project_recode_lookup):
    """Clean the raw climate-finance project records used in later outputs.

    The main stages are:
    1. Rename columns into short, analysis-friendly labels.
    2. Keep only the study years.
    3. Harmonize text labels across major categorical variables.
    4. Fill selected missing sector, subsector, and financing labels.
    """
    # 1. Rename raw source columns and standardize the final column labels.
    raw_crdf = clean.rename_columns(
        raw_crdf,
        CRDF_COLUMN_RENAMES,
        standardize_column_names = True,
    )

    # 2. Restrict the dataset to the study period used in this project.
    raw_crdf = explore.filter_rows_by_range(raw_crdf, "year", START_YEAR, END_YEAR)

    # 3. Correct a known region-label inconsistency before applying the broader
    # region recoding rules.
    middle_east_overrides = {
        country_name: "Middle East"
        for country_name in COUNTRIES_TO_REASSIGN_TO_MIDDLE_EAST
    }
    raw_crdf = clean.recode_values(
        raw_crdf,
        "region",
        middle_east_overrides,
        based_on = "recipient",
    )

    # 4. Harmonize category labels so later grouping and comparison steps use
    # one consistent coding scheme.
    raw_crdf = clean.recode_values(raw_crdf, "provider_type", PROVIDER_TYPE_RECODING)
    raw_crdf = clean.recode_values(raw_crdf, "provider", PROVIDER_RECODING)
    raw_crdf = clean.recode_values(raw_crdf, "recipient", RECIPIENT_RECODING)
    raw_crdf = clean.recode_values(raw_crdf, "region", REGION_RECODING)
    raw_crdf = clean.recode_values(raw_crdf, "concessionality", CONCESSIONALITY_RECODING)

    for marker_column in RIO_MARKER_COLUMNS:
        raw_crdf = clean.recode_values(raw_crdf, marker_column, RIO_MARKER_RECODING)

    # 5. Remove numeric prefixes from sector labels such as "123. Energy".
    raw_crdf = clean.remove_prefix(raw_crdf, "sector")

    # 6. Build project-title lookup tables for sector and subsector values that
    # can be recovered from the manual recoding reference file.
    sector_lookup = dict(
        zip(project_recode_lookup["Project"], project_recode_lookup["Sector"])
    )
    subsector_lookup = dict(
        zip(project_recode_lookup["Project"], project_recode_lookup["Subsector"])
    )

    # 7. Fill missing sector values from the manual lookup when possible.
    raw_crdf = clean.fill_missing_values(
        raw_crdf,
        "sector",
        recoded_value = raw_crdf["project"].map(sector_lookup),
        condition = raw_crdf["project"].isin(sector_lookup),
    )

    # 8. For any remaining missing sector values, use the project's fallback
    # label so downstream grouping does not depend on missing text fields.
    raw_crdf = clean.fill_missing_values(
        raw_crdf,
        "sector",
        recoded_value = "Unallocated / Unspecified",
        condition = True,
    )

    # 9. If the sector is unallocated/unspecified, force the subsector into a
    # matching catch-all label as well.
    raw_crdf = clean.fill_missing_values(
        raw_crdf,
        "subsector",
        recoded_value = "Sectors not specified",
        condition = raw_crdf["sector"].eq("Unallocated / Unspecified"),
    )

    # 10. Otherwise, recover missing subsector values from the manual lookup.
    raw_crdf = clean.fill_missing_values(
        raw_crdf,
        "subsector",
        recoded_value = raw_crdf["project"].map(subsector_lookup),
        condition = (
            raw_crdf["project"].isin(subsector_lookup)
            & raw_crdf["sector"].ne("Unallocated / Unspecified")
        ),
    )

    # 11. Fill selected financing labels based on already-harmonized
    # concessionality categories.
    raw_crdf = clean.fill_missing_values(
        raw_crdf,
        "financing_type",
        recoded_value = "Export credit",
        condition = raw_crdf["concessionality"].eq(
            "Non-concessional, Export credits"
        ),
    )

    raw_crdf = clean.fill_missing_values(
        raw_crdf,
        "financing_type",
        recoded_value = "Unspecified",
        condition = raw_crdf["concessionality"].eq(
            "Non-concessional, Development NOT the main objective"
        ),
    )

    return raw_crdf


def prepare_mvi_dataset(mvi_vulnerability_data, mvi_resilience_data):
    """Combine the two Multidimensional Vulnerability Index source sheets.

    1. Join the vulnerability sheet and the resilience sheet by country and ISO.
    2. Rename the resulting indicators into short column names used elsewhere.
    """
    return clean.merge_data(
        data_1 = mvi_vulnerability_data,
        data_2 = mvi_resilience_data,
        key = ["Country", "ISO"],
        columns_to_rename = MVI_COLUMN_RENAMES,
    )


def assign_recipient_iso_codes(project_level_data):
    """Assign ISO3 codes while preserving non-country recipient rows.

    1. Identify recipient labels that refer to regional aggregates or
       unspecified groupings rather than individual countries.
    2. Assign ISO3 codes only to country rows.
    3. Keep non-country rows as `'not found'` so they are not mistaken for one
       country in later merges.
    """
    # 1. Flag recipient rows that should not be converted into one country ISO.
    recipient_rows_without_country_iso = project_level_data["recipient"].str.contains(
        r"\b(?:regional|unspecified)\b",
        case = False,
        na = False,
    )

    # 2. Initialize the column, then fill ISO3 values only where conversion
    # should be attempted.
    project_level_data = project_level_data.copy()
    project_level_data["ISO"] = "not found"
    project_level_data.loc[~recipient_rows_without_country_iso, "ISO"] = coco.convert(
        names = project_level_data.loc[~recipient_rows_without_country_iso, "recipient"],
        to = "ISO3",
    )
    return project_level_data


def build_recipient_year_panel(project_level_data):
    """Aggregate project-level commitments into recipient-by-year totals.

    1. Sum project commitments within each recipient-year combination.
    2. Keep region and income labels attached to each recipient panel row.
    3. Rename year columns with a `USD_` prefix to show that they hold annual
       commitment amounts before any per-capita calculation.
    """
    # 1. Build one row per recipient and one commitment column per year.
    recipient_year_panel = (
        project_level_data.pivot_table(
            index = ["recipient", "ISO", "region", "income"],
            columns = "year",
            values = "commitment",
            aggfunc = "sum",
            fill_value = 0,
        )
        .reset_index()
    )

    # 2. Mark year columns explicitly as annual commitment amounts in USD terms.
    recipient_year_panel.columns = [
        f"USD_{column_name}" if isinstance(column_name, int) else column_name
        for column_name in recipient_year_panel.columns
    ]
    return recipient_year_panel


def prepare_population_dataset(population_data):
    """Rename yearly population columns so they align with the recipient panel."""
    return clean.rename_columns(population_data, POPULATION_COLUMN_RENAMES)


def prepare_sids_dataset(sids_reference_data):
    """Rename the Small Island Developing States reference columns for merging."""
    return clean.rename_columns(sids_reference_data, SIDS_COLUMN_RENAMES)


def build_per_capita_dataset(
    recipient_year_panel,
    population_data,
    vulnerability_data,
    sids_reference_data,
):
    """Build the per-capita analysis dataset from yearly recipient totals.

    1. Keep only rows that represent individual countries with usable ISO codes.
    2. Merge in yearly population data, vulnerability indicators, and the SIDS
       reference file.
    3. Apply a few manual SIDS corrections where the reference file is missing.
    4. Calculate:
       - yearly commitment-per-capita columns such as `CRDF_PER_CAPITA_2015`
       - total aggregate commitment (2015-2023 combined), in unit USD
       - sum of the yearly per-capita values
    """
    # 1. Merge the country-level recipient panel with the supporting reference
    # tables needed for per-capita calculations.
    per_capita_data = (
        clean.merge_data(
            data_1 = recipient_year_panel[recipient_year_panel["ISO"] != "not found"],
            data_2 = population_data,
            key = "ISO",
            data_3 = vulnerability_data,
            data_4 = sids_reference_data,
            join_type = ["inner", "left", "left"],
            columns_to_remove = ["ISO", "Country", "recipient", "recipient_y"],
            columns_to_rename = {"recipient_x": "recipient"},
            columns_to_rearrange = ["recipient", "region", "SIDS", "income"],
        )
        .assign(SIDS = lambda data: data["SIDS"].astype("Int64"))
    )

    # 2. Fill a small number of known SIDS values that are missing in the raw
    # reference file but needed in the final output.
    for country_name, sids_value in SIDS_MANUAL_RECODING.items():
        per_capita_data = clean.fill_missing_values(
            per_capita_data,
            "SIDS",
            recoded_value = sids_value,
            condition = per_capita_data["recipient"] == country_name,
        )

    # 3. Derive the yearly and full-period per-capita measures.
    per_capita_data = clean.calculate_commitment_per_capita(
        per_capita_data,
        years = YEARS,
    )

    return per_capita_data


def build_final_analysis_dataset(project_level_data, vulnerability_data):
    """Attach vulnerability indicators to the cleaned project-level dataset.

    1. Merge the project-level climate-finance table with the vulnerability
       indicators by ISO code.
    2. Remove merge-only columns that are not meant to stay in the final file.
    """
    return clean.merge_data(
        data_1 = project_level_data,
        data_2 = vulnerability_data,
        key = "ISO",
        join_type = ["left"],
        columns_to_remove = ["Country", "ISO", "project"],
    )


def build_analysis_output_tables(final_project_data, per_capita_data):
    """Build grouped analysis tables from the two cleaned final datasets."""
    return summarize.build_analysis_tables(final_project_data, per_capita_data)


def save_final_outputs(final_project_data, per_capita_data, analysis_tables = None):
    """Write the canonical final outputs plus optional analysis-ready tables.

    1. Save the project-level final dataset.
    2. Save the per-capita final dataset.
    3. Save optional summary tables used for later exploration.
    4. Remove temporary `[TEST]` files from the final-data folder.
    """
    # 1. Save the main project-level output.
    access.save_to_csv(
        final_project_data,
        "Climate Finance (2015-2023)",
        folder_name = "1_Data/final",
        for_testing = False,
    )

    # 2. Save the recipient-level per-capita output.
    access.save_to_csv(
        per_capita_data,
        "Climate Finance PER CAPITA",
        folder_name = "1_Data/final",
        for_testing = False,
    )

    if analysis_tables is not None:
        # 3. Save the optional grouped tables in a separate analysis folder.
        summarize.save_analysis_tables(analysis_tables)

    # 4. Remove any leftover temporary test files from prior exploratory runs.
    access.delete_test_files(folder_name = "1_Data/final")
