"""Locate project folders, and read or save files."""

import pandas as pd
from pathlib import Path



# --------------------------------------------------
# A. DEFINE THE PROJECT ROOT DIRECTORY & PATHS TO ALL FOLDERS
# --------------------------------------------------

ROOT_DIRECTORY = Path(__file__).resolve().parents[2]
DATA_FOLDER = ROOT_DIRECTORY / "1_Data"
CODE_FOLDER = ROOT_DIRECTORY / "2_Code"
OUTPUT_FOLDER = ROOT_DIRECTORY / "3_Output"



# --------------------------------------------------
# B. ACCESS/NAVIGATE THE PROJECT WORKSPACE
# --------------------------------------------------

def _resolve_folder_path(folder_name):
    """Construct the path to one of the main project folders.

    Valid root-level folder names:
        '1_Data', '2_Code', '3_Output'

    The helper also accepts nested project-relative paths such as
    '1_Data/raw', '1_Data/final', or '1_Data/pivot_tables'.
    """

    location = {
        "1_Data": DATA_FOLDER,
        "2_Code": CODE_FOLDER,
        "3_Output": OUTPUT_FOLDER
    }

    return location.get(folder_name, ROOT_DIRECTORY / folder_name)


def locate_folder(folder_name):
    """Return a full path of the given folder."""
    folder_path = _resolve_folder_path(folder_name)
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder does not exist: {folder_path}")
    return folder_path



def locate_file(folder_name, file_name, file_type):
    """Construct a full path of the given file."""

    folder_path = locate_folder(folder_name)
    return folder_path / f"{file_name}.{file_type}"



# --------------------------------------------------
# C. LOAD TABULAR DATASETS
# --------------------------------------------------

def set_dataframe_name(data, dataframe_name):
    """Assign a label/name (metadata) to the given dataframe.
    The label lives in ``data.attrs``.
    """
    if not isinstance(dataframe_name, str) or not dataframe_name:
        raise ValueError("dataframe_name must be provided as a non-empty string.")

    data.attrs['dataframe_name'] = dataframe_name
    return data

def _inspect_loaded_data(data, inspect_name = None, save_missing_rows = False):
    """
    Display a high-level summary of the dataset once it is loaded.
    """
    from . import explore

    data = set_dataframe_name(data, inspect_name or 'data')
    explore.inspect_data(data, save_missing_rows = save_missing_rows)
    return data


def _inspect_saved_data(data, inspect_name):
    """
    Once the dataset has been cleaned,
    inspect 1) the dimensionality of the dataset and 2) its missing values
    """
    from . import explore

    data = set_dataframe_name(data, inspect_name)
    explore.inspect_missing_values(data, save_missing_rows = False)
    return data


def load_table(
        file_name,
        file_type,
        sheet = None,
        columns = None,
        rows = None,
        folder_name = "1_Data",
        inspect = True,
        inspect_name = None,
        save_missing_rows = False,
        **kwargs):
    """Load a tabular file from one of the project folders.

    Parameters
    ----------
    file_type : str
        Supported values are `'excel'`, `'xlsx'`, and `'csv'`.
    """

    resolved_file_type = "xlsx" if file_type == "excel" else file_type
    file_path = locate_file(folder_name, file_name, resolved_file_type)

    if file_type in {"excel", "xlsx"}:
        data = pd.read_excel(
            file_path,
            sheet_name = sheet,
            usecols = columns,
            **kwargs,
        )
    elif file_type == "csv":
        data = pd.read_csv(
            file_path,
            usecols = columns,
            nrows = rows,
            **kwargs,
        )
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    data = set_dataframe_name(data, inspect_name or file_name)

    if inspect:
        data = _inspect_loaded_data(
            data,
            inspect_name = inspect_name,
            save_missing_rows = save_missing_rows,
        )

    return data


# --------------------------------------------------
# D. MANAGE FILES WITHIN THE PROJECT WORKSPACE
# --------------------------------------------------

def add_prefix_to_file(file_name, prefix = None):
    """
    Append a prefix to the file name, if provided.
    Purpose: To indicate which phase of the project pipeline it relates to

    Valid prefix:
        '[TEST]', '[FINAL]'

    N.B.: Files used for testing purposes will later be deleted.
    """

    if prefix:
        return f"{prefix} {file_name}"
    return file_name



def save_to_csv(
        data,
        file_name,
        folder_name = "1_Data",
        for_testing = False,
        prefix = None,
        dataframe_name = None):
    """
    Save a dataframe to CSV with a configurable filename prefix.

    Parameters
    ----------
    data : pd.DataFrame
        Dataframe to save.
    file_name : str
        Base file name without extension.
    folder_name : str, default "1_Data"
        Project folder or subfolder in which the file should be stored.
    for_testing : bool, default False
        If True, use the '[TEST]' prefix unless a custom prefix is supplied.
    prefix : str or None, default None
        Optional custom prefix such as '[SUMMARY]'.
        When omitted, the helper preserves the historical behavior:
        '[FINAL]' for production outputs and '[TEST]' for temporary files.
    dataframe_name : str or None, default None
        Label to use in the printed save-time inspection summary.
        If omitted, the helper reuses `file_name`.
    """

    output_label = dataframe_name or file_name

    if prefix is None:
        prefix = "[TEST]" if for_testing else "[FINAL]"
    file_name = add_prefix_to_file(file_name, prefix)
    file_path = locate_file(folder_name, file_name, "csv")
    data.to_csv(file_path, index = False)

    data = set_dataframe_name(data, output_label)

    print(f"Data saved in '{folder_name}' folder as: {file_name}.csv")
    _inspect_saved_data(data, inspect_name = output_label)
    return file_path


def save_to_excel(
        data,
        file_name,
        sheet_name = "Sheet1",
        folder_name = "1_Data",
        prefix = None,
        dataframe_name = None,
        apply_autofilter = True,
        freeze_header = True):
    """
    Save a dataframe to Excel and optionally enable spreadsheet filters.
    """

    output_label = dataframe_name or file_name
    file_name = add_prefix_to_file(file_name, prefix)
    file_path = locate_file(folder_name, file_name, "xlsx")

    with pd.ExcelWriter(file_path, engine = "openpyxl") as writer:
        data.to_excel(writer, sheet_name = sheet_name, index = False)
        worksheet = writer.sheets[sheet_name]

        if apply_autofilter and data.empty:
            worksheet.auto_filter.ref = "A1"
        elif apply_autofilter:
            worksheet.auto_filter.ref = worksheet.dimensions

        if freeze_header:
            worksheet.freeze_panes = "A2"

    data = set_dataframe_name(data, output_label)

    print(f"Data saved in '{folder_name}' folder as: {file_name}.xlsx")
    _inspect_saved_data(data, inspect_name = output_label)
    return file_path



def delete_test_files(folder_name = "1_Data"):
    """
    Within the specified folder, remove all files beginning with '[TEST]'.
    """

    # Search for files whose name begins with [TEST]
    folder_path = locate_folder(folder_name)
    test_files = list(folder_path.glob("[[]TEST[]]*"))

    # If no such files exist, inform the user
    if not test_files:
        print(f"--- There were 0 files to remove in the '{folder_name}' folder. ---")
        return

    # Delete test files if they exist and keep count of them
    deleted_count = 0

    for file_path in test_files:

        try:
            file_path.unlink()
            print(f"Deleted: {file_path.name}")
            deleted_count += 1

        except Exception as error:
            print(f"Error deleting {file_path.name}: {error}")

    print(f"\n--- Clean up complete! {deleted_count} file(s) removed from '{folder_name}'. ---")
