import numpy as np
import pandas as pd
import pytask

from src.config import BLD
from src.config import SRC


def p_renaming_columns(df):
    new_names = pd.read_csv(
        SRC / "data_management/PENDDAT/penddat_renaming.csv", sep=";"
    )["new_name"]
    renaming_dict = dict(zip(df.columns, new_names))
    renaming_dict = {
        k: v for k, v in renaming_dict.items() if pd.notna(v)
    }  # Deleting NA values
    return df.rename(columns=renaming_dict)


def p_replacing_negative(data_set, negatives=list(range(-1, -11, -1))):
    for i in negatives:
        data_set[data_set == i] = np.nan
    return data_set


def p_clean_data(df, negatives=list(range(-1, -11, -1))):
    new_names = pd.read_csv(
        SRC / "data_management/PENDDAT/penddat_renaming.csv", sep=";"
    )["new_name"]
    renaming_dict = dict(zip(df.columns, new_names))
    renaming_dict = {
        k: v for k, v in renaming_dict.items() if pd.notna(v)
    }  # Deleting NA values
    df = df.rename(columns=renaming_dict)
    for i in negatives:
        df[df == i] = np.nan
    return df


# trial to make a global cleaning rather than data specific
""" def _clean_data(data_set, negatives=list(range(-1, -11, -1))):
    new_names = pd.read_csv(
        SRC / f"data_management/{data_set}/{data_set}_renaming.csv", sep=";"
    )["new_name"]
    renaming_dict = dict(zip(df.columns, new_names))
    renaming_dict = {
        k: v for k, v in renaming_dict.items() if pd.notna(v)
    }  # Deleting NA values
    df = df.rename(columns=renaming_dict)
    for i in negatives:
        df[df == i] = np.nan
    return df
    for id in ["p_id", "hh_id", "wave"]:
        if id in  """


@pytask.mark.depends_on(SRC / "original_data/PENDDAT_cf_W11.dta")
@pytask.mark.produces(BLD / "p_clean.pickle")
def task_clean_penddat(depends_on, produces):
    df = pd.read_stata(depends_on, convert_categoricals=False)
    # df = p_renaming_columns(df)
    # df = p_replacing_negative(df).set_index(["p_id", "hh_id", "wave"])
    df = p_clean_data(df).set_index(["p_id", "hh_id", "wave"]).sort_index()
    df.to_pickle(produces)
