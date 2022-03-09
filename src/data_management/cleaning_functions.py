import glob
from pathlib import Path
from sys import platform

import numpy as np
import pandas as pd
import yaml

from src.config import SRC


def get_names_dataset(path=SRC / "original_data"):
    if platform == "win32":
        a = r"\*"
        b = "\\"
    elif platform == "darwin":
        a = r"/*"
        b = "/"

    files = list(glob.glob(str(path) + f"{a}"))
    name = []
    for i in range(len(files)):
        # if any(x.isupper() for x in files[i].split(f"{b}")[-1].split("_")[0]):
        name.append(files[i].split(f"{b}")[-1].split("_")[0])
    # else:
    #    name.append(
    #       files[i].split(f"{b}")[-1].split("_")[0]
    #      + "_"
    #     + files[i].split(f"{b}")[-1].split("_")[1]
    # )
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


def reverse_code(df):
    for i in df:
        if (f"{i}" in df.filter(regex="_neg")) is True:
            new_column_name = i.replace("_neg", "")
            value_to_be_used = df[i].max() + 1
            df[new_column_name] = value_to_be_used - df[i]
    return df


def average_big5(df):
    facets_b5 = ["ext", "agree", "consc", "neu", "open"]
    for i in facets_b5:
        df[f"b5_{i}"] = (
            df[df.columns.drop(list(df.filter(regex="_neg")))]
            .filter(regex=f"b5_{i}")
            .mean(axis=1)
        )
    return df


def average_eri(df):
    facets_eri = ["effort", "reward"]
    for i in facets_eri:
        df[f"eri_{i}"] = (
            df[df.columns.drop(list(df.filter(regex="_neg")))]
            .filter(regex=f"eri_{i}")
            .mean(axis=1)
        )
    return df


def average_genrole(df):
    df["genrole_traditional"] = df.filter(regex="traditional").mean(axis=1)
    return df


def create_dummies(df_p, df_h, dummies_p=None, dummies_h=None):
    # df_p = pd.read_pickle(BLD / "PENDDAT_clean.pickle")
    # df_h = pd.read_pickle(BLD / "HHENDDAT_clean.pickle")
    if dummies_p is None:
        dummies_p = Path(SRC / "data_management/dummies/PENDDAT_dummies.yaml")
    if dummies_h is None:
        dummies_h = Path(SRC / "data_management/dummies/HHENDDAT_dummies.yaml")
    with open(dummies_p) as stream:
        dummies_p = yaml.safe_load(stream)
    with open(dummies_h) as stream:
        dummies_h = yaml.safe_load(stream)
    for dummies in dummies_p:
        if dummies == "PG0100":
            df_p[f"{dummies}_dummy"] = (df_p[f"{dummies}"] > 0).astype(int)
            df_p.loc[df_p[f"{dummies}"].isna(), [f"{dummies}_dummy"]] = np.nan
        elif dummies == "migration":  # becuase migration has reversed value in dummies
            df_p = pd.concat(
                [
                    df_p,
                    pd.get_dummies(
                        df_p[f"{dummies}"], prefix=f"{dummies}", dummy_na=True
                    ).rename(columns={f"{dummies}_2.0": f"{dummies}_dummy"}),
                ],
                axis=1,
            ).drop(f"{dummies}_1.0", axis=1)
            df_p.loc[df_p[f"{dummies}_nan"] == 1, [f"{dummies}_dummy"]] = np.nan
            df_p.drop(f"{dummies}_nan", axis=1, inplace=True)
        else:
            df_p = pd.concat(
                [
                    df_p,
                    pd.get_dummies(
                        df_p[f"{dummies}"], prefix=f"{dummies}", dummy_na=True
                    ).rename(columns={f"{dummies}_1.0": f"{dummies}_dummy"}),
                ],
                axis=1,
            ).drop(f"{dummies}_2.0", axis=1)
            df_p.loc[df_p[f"{dummies}_nan"] == 1, [f"{dummies}_dummy"]] = np.nan
            df_p.drop(f"{dummies}_nan", axis=1, inplace=True)
    for dummies in dummies_h["others"]:
        df_h = pd.concat(
            [
                df_h,
                pd.get_dummies(
                    df_h[f"{dummies}"], prefix=f"{dummies}", dummy_na=True
                ).rename(columns={f"{dummies}_1.0": f"{dummies}_dummy"}),
            ],
            axis=1,
        ).drop(f"{dummies}_2.0", axis=1)
        df_h.loc[df_h[f"{dummies}_nan"] == 1, [f"{dummies}_dummy"]] = np.nan
        df_h.drop(f"{dummies}_nan", axis=1, inplace=True)
    return (df_p, df_h)


def create_dummies_depr(df_h, dummies_h=None):
    if dummies_h is None:
        dummies_h = Path(SRC / "data_management/dummies/HHENDDAT_dummies.yaml")
    with open(dummies_h) as stream:
        dummies_h = yaml.safe_load(stream)
    for dummies in dummies_h["deprivation"]:
        df_h.loc[df_h[f"{dummies}b"] == 1, f"{dummies}_dummy"] = 1
        df_h.loc[df_h[f"{dummies}b"] == 2, f"{dummies}_dummy"] = 0
        df_h.loc[df_h[f"{dummies}a"] == 1, f"{dummies}_dummy"] = 0
    return df_h
