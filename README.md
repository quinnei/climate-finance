# Climate Finance

This repository contains summary tables and a written analysis of climate
finance flows throughout 2015-2023.


## Repository Structure

- [REPORT](https://github.com/quinnei/climate-finance/blob/main/3_Output/Climate-Finance-2015-2023.pdf)
- [CODEBOOK & CLEANED DATA](https://github.com/quinnei/climate-finance/tree/main/1_Data/final)
  - An overview of the data cleaning / data processing procedures. 
  - *Project*-level data on climate finance flows from 2015-2023.
  - *Country*-level data, showing recipient country profiles and amounts of
    climate finance, both in terms of the 1) raw total and 2) per capita
    allocations.
- [SUMMARY TABLES](https://github.com/quinnei/climate-finance/tree/main/1_Data/pivot_tables)
  - Excel files with built-in filters. Equivalent to pivot tables that you
    would get from aggregating the project-level dataset.
- [PYTHON SCRIPTS]   
  - [Python code](https://github.com/quinnei/climate-finance/tree/main/2_Code) used to clean raw data and construct summary tables.
  - [Python code](https://github.com/quinnei/climate-finance/blob/main/3_Output/quarto/figures_and_tables.py) used to generate figures and tables for the report.


## Instructions

1. Clone the repo.

```bash
git clone https://github.com/quinnei/climate-finance.git
cd climate-finance
```

2. To replicate the data product (i.e. reproduce the cleaned datasets and summary tables), download raw source files from the following link:
- **Climate Finance Commitments, Recipient perspective**:
  [`CRDF-RP-all years-2000-2023.xlsx`](https://webfs.oecd.org/climate/RecipientPerspective/)
- **Multidimensional Vulnerability Index (MVI)**:
  [`MVI Results Data (Microsoft Excel)`](https://www.un.org/ohrlls/mvi/documents)
- **Small Island Developing States (SIDS)** :
  [`DAC and CRS code lists.xlsx`](https://www.oecd.org/en/data/insights/data-explainers/2024/10/resources-for-reporting-development-finance-statistics.html)
- **Population, Total**:
  [`Population 2015-2023.csv`](https://data.worldbank.org/indicator/SP.POP.TOTL)
- **Manuals for filling in missing values**:
  [[RAW] Sector.csv](https://github.com/quinnei/climate-finance/blob/main/1_Data/raw/%5BRAW%5D%20Sector.csv)
  - File created by the repo owner. No need for downloads, as it comes with `1_Data/raw` folder of the repo.
  - Contains a lookup table that matches projects with their respective
    'sector' and 'subsector' categories. Deduced from project metadata.

3. Save the raw source files in the existing `1_Data/raw` folder and rename as:
- **Climate Finance Commitments, Recipient perspective**: `[RAW] Climate Related Development Finance (Recipient POV).xlsx`
- **Multidimensional Vulnerability Index (MVI)**: `[RAW] MVI.xlsx`
- **Small Island Developing States (SIDS)** : `[RAW] OECD DAC CRS.xlsx`
- **Population, Total**: `[RAW] Population.csv`

4. Install the project dependencies.

```bash
uv sync
```

5. Run `main.py`.

```bash
uv run python 2_Code/main.py
```


## Notes

- In `[FINAL] Climate Finance (2015-2023).csv`, `commitment` is expressed in
  `thousand USD`.
- In `[FINAL] Climate Finance PER CAPITA.csv`, commitment-related values are
  expressed in `USD` or `USD per capita`.
