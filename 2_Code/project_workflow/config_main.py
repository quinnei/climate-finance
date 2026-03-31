"""Store the settings and recoding rules used to build this project's datasets."""


# =======================================================================================
# I. TIME SPAN
# =======================================================================================

# Keep the year range in one place so that all yearly transformations remain
# aligned and are easier to update in future panel-data projects.
START_YEAR = 2015
END_YEAR = 2023
YEARS = range(START_YEAR, END_YEAR + 1)


# =======================================================================================
# II. INPUT FILE DEFINITIONS
# =======================================================================================

# Each entry describes how a raw input file should be read.
FILE_INFO = {
    'CRDF': dict(
        folder_name = '1_Data/raw',
        file_type = 'excel',
        file_name = '[RAW] Climate Related Development Finance (Recipient POV)',
        sheet = 'All',
        columns = [
            'Year',
            'Provider Type',
            'Provider (detailed)',
            'Recipient',
            'Recipient Region',
            'Recipient Income Group (OECD Classification)',
            'Concessionality',
            'Climate objective (applies to Rio-marked data only) or climate component',
            'Adaptation objective (applies to Rio-marked data only)',
            'Mitigation objective (applies to Rio-marked data only)',
            'Climate-related development finance - Commitment - Current USD thousand',
            'Sector',
            'Sub-sector',
            'Financial Instrument',
            'Project Title',
        ],
    ),
    'MVI_vulnerability': dict(
        folder_name = '1_Data/raw',
        file_type = 'excel',
        file_name = '[RAW] MVI',
        sheet = 'Structural vulnerability',
        columns = [
            'Country',
            'ISO',
            'Economic vulnerability',
            'Environmental vulnerability',
        ],
    ),
    'MVI_resilience': dict(
        folder_name = '1_Data/raw',
        file_type = 'excel',
        file_name = '[RAW] MVI',
        sheet = 'Lack of structural resilience',
        columns = [
            'Country',
            'ISO',
            'Lack of Economic Resilience',
            'Lack of Environmental Resilience',
        ],
    ),
    'population': dict(
        folder_name = '1_Data/raw',
        file_type = 'csv',
        file_name = '[RAW] Population',
        columns = [
            'Country Name',
            'Country Code',
            *[f'{year} [YR{year}]' for year in YEARS],
        ],
        rows = 217,
    ),
    'projects_to_recode': dict(
        folder_name = '1_Data/raw',
        file_type = 'csv',
        file_name = '[RAW] Sector',
    ),
    'SIDS': dict(
        folder_name = '1_Data/raw',
        file_type = 'excel',
        file_name = '[RAW] OECD DAC CRS',
        sheet = 'Recipient',
        columns = ['Recipient name (EN)', 'ISOcode', 'SIDS'],
    ),
}


# =======================================================================================
# III. COLUMN RENAMES
# =======================================================================================

CRDF_COLUMN_RENAMES = {
    'Provider (detailed)': 'provider',
    'Recipient Region': 'region',
    'Recipient Income Group (OECD Classification)': 'income',
    'Climate objective (applies to Rio-marked data only) or climate component': 'climate_obj',
    'Adaptation objective (applies to Rio-marked data only)': 'adaptation_obj',
    'Mitigation objective (applies to Rio-marked data only)': 'mitigation_obj',
    'Climate-related development finance - Commitment - Current USD thousand': 'commitment',
    'Financial Instrument': 'financing_type',
    'Project Title': 'project',
}

MVI_COLUMN_RENAMES = {
    'Economic vulnerability': 'ECON_vulnerability',
    'Environmental vulnerability': 'ENV_vulnerability',
    'Lack of Economic Resilience': 'ECON_lack_resilience',
    'Lack of Environmental Resilience': 'ENV_lack_resilience',
}

POPULATION_COLUMN_RENAMES = {
    'Country Name': 'recipient',
    'Country Code': 'ISO',
    **{f'{year} [YR{year}]': f'POP_{year}' for year in YEARS},
}

SIDS_COLUMN_RENAMES = {
    'Recipient name (EN)': 'recipient',
    'ISOcode': 'ISO',
}


# =======================================================================================
# IV. PROJECT-SPECIFIC RECODING RULES
# =======================================================================================

COUNTRIES_TO_REASSIGN_TO_MIDDLE_EAST = [
    'Iran',
    'Iraq',
    'Jordan',
    'Lebanon',
    'Syrian Arab Republic',
    'West Bank and Gaza Strip',
    'Yemen',
]

PROVIDER_TYPE_RECODING = {
    'DAC member': 'Bilateral donor (DAC)',
    'Non-DAC member': 'Bilateral donor (Non-DAC)',
    'Multilateral development bank': 'Multilateral donor (MDB)',
    'Other multilateral': 'Multilateral donor (Non-MDB)',
    'Private donor': 'Private donor (NGOs/Philanthropy/Charity)',
}

PROVIDER_RECODING = {
    'EU institutions (EIB)': 'European Investment Bank (EIB)',
    'EU Institutions (excl. EIB)': 'EU Institutions (Excluding EIB)',
}

RECIPIENT_RECODING = {
    "China (People's Republic of)": 'China',
}

REGION_RECODING = {
    'Far East Asia': 'Asia',
    'South & Central Asia': 'Asia',
    'North of Sahara': 'Africa',
    'South of Sahara': 'Africa',
    'America': 'Latin America & the Caribbean',
    'Caribbean & Central America': 'Latin America & the Caribbean',
    'South America': 'Latin America & the Caribbean',
    'Unspecified': 'Developing countries (General; Unspecified)',
}

CONCESSIONALITY_RECODING = {
    'Concessional and developmental': 'Concessional, Development as the main objective',
    'Private concessional': 'Concessional, From NGOs/philanthropy/charity',
    'Officially supported export credits': 'Non-concessional, Export credits',
    'Private sector instruments': 'Non-concessional, Financial instruments to promote private sector development in developing countries',
    'Not concessional or not primarily developmental': 'Non-concessional, Development NOT the main objective',
    'Not specified': 'Unspecified',
}

RIO_MARKER_RECODING = {
    'Principal': 'Primary',
    'Significant': 'Important but secondary',
    'Climate components': 'Some climate component',
    'Not targeted/Not screened': 'Not a target/Not reviewed',
}

RIO_MARKER_COLUMNS = ['climate_obj', 'adaptation_obj', 'mitigation_obj']

SIDS_MANUAL_RECODING = {
    'Antigua and Barbuda': 1,
    'Chile': 0,
    'Seychelles': 1,
    'Uruguay': 0,
}
