import pandas as pd

def fetch_data_from_file(filepath, delimiter=","):
    """
    Load data from a CSV file with a specified delimiter.
    """
    return pd.read_csv(filepath, delimiter=delimiter)

def save_data_to_file(dataframe, filepath, delimiter=","):
    """
    Save a DataFrame to a CSV file with a specified delimiter.
    """
    dataframe.to_csv(filepath, index=False, sep=delimiter)
