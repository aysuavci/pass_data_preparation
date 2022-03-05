import glob
from pathlib import Path
from sys import platform

import numpy as np
import pandas as pd
import pytask

from src.config import BLD
from src.config import SRC


def get_names_dataset(path=SRC / "original_data"):
    if platform == "win32":
        a = r"\*"
        b = "\\"
    elif platform == "darwin":
        a = r"/*"
        b = "/"

    files = list(glob.glob(str(path) + f"{a}"))
    # [file for file in glob.glob(path + f"\*")]
    name = []
    for i in range(len(files)):
        if any(x.isupper() for x in files[i].split(f"{b}")[-1].split("_")[0]):
            name.append(files[i].split(f"{b}")[-1].split("_")[0])
        else:
            # a = files[i].split("\\")[6].split("_")[0:2]
            name.append(
                files[i].split(f"{b}")[-1].split("_")[0]
                + "_"
                + files[i].split(f"{b}")[-1].split("_")[1]
            )
    return name


def p_renaming_columns(df):
    new_names = pd.read_csv(
        SRC / "data_management/PENDDAT/penddat_renaming.csv", sep=";"
    )["new_name"]
    renaming_dict = dict(zip(df.columns, new_names))
    renaming_dict = {
        k: v for k, v in renaming_dict.items() if pd.notna(v)
    }  # Deleting NA values
    return df.rename(columns=renaming_dict)


def p_replacing_negative(data_set, negatives=None):
    if negatives is None:
        negatives = list(range(-1, -11, -1))
    for i in negatives:
        data_set[data_set == i] = np.nan
    return data_set


""" def p_clean_data(df, negatives=None):
    if negatives is None:
        negatives = list(range(-1, -11, -1))

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
    return df """


def clean_data(df, i, negatives=None):
    if negatives is None:
        negatives = list(range(-1, -11, -1))

    new_names = pd.read_csv(SRC / f"data_management/{i}/{i}_renaming.csv", sep=";")[
        "new_name"
    ]
    renaming_dict = dict(zip(df.columns, new_names))
    renaming_dict = {
        k: v for k, v in renaming_dict.items() if pd.notna(v)
    }  # Deleting NA values
    df = df.rename(columns=renaming_dict)
    for i in negatives:
        df[df == i] = np.nan
    return df


""" @pytask.mark.depends_on(SRC / "original_data/PENDDAT_cf_W11.dta")
@pytask.mark.produces(BLD / "p_clean.pickle")
def task_clean_penddat(depends_on, produces):
    df = pd.read_stata(depends_on, convert_categoricals=False)
    # df = p_renaming_columns(df)
    # df = p_replacing_negative(df).set_index(["p_id", "hh_id", "wave"])
    df = p_clean_data(df).set_index(["p_id", "hh_id", "wave"]).sort_index()
    df.to_pickle(produces) """


def reverse_code_big5(df):
    for i in df:
        if (f"{i}" in df.filter(regex="_neg")) is True:
            new_column_name = i.replace("_neg", "")
            df[new_column_name] = (
                6 - df[i]
            )  # Subtrack 6 to reverse code(6-1(lowest)=5(highest))
    return df


def average_big5(df):
    facets = ["ext", "agree", "consc", "neu", "open"]
    for i in facets:
        df[f"b5_{i}"] = (
            df[df.columns.drop(list(df.filter(regex="_neg")))]
            .filter(regex=f"b5_{i}")
            .mean(axis=1)
        )
    return df


@pytask.mark.depends_on(SRC / "original_data")
@pytask.mark.produces(BLD)
def task_cleaning(depends_on, produces):
    names = get_names_dataset()
    for i in names:
        df = pd.read_stata(
            str(Path(depends_on)) + f"/{i}_cf_W11.dta", convert_categoricals=False
        )
        if "p_id" in df.columns:
            df = clean_data(df, i).set_index(["p_id", "hh_id", "wave"]).sort_index()
            df = reverse_code_big5(df)
            df = average_big5(df)
        else:
            df = clean_data(df, i).set_index(["hh_id", "wave"]).sort_index()
        df.to_pickle(str(Path(produces)) + f"/{i}_clean.pickle")


""" @pytask.mark.try_last
@pytask.mark.depends_on(BLD / "PENDDAT_clean.pickle")
@pytask.mark.produces(BLD)
def task_scaling(depends_on, produces):
    df=pd.read_pickle(depends_on) #read the clean data
    df=reverse_code_big5(df)
    df=average_big5(df)
    df.to_pickle(produces / "PENDDAT_clean.pickle")
 """
