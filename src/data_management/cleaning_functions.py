import glob
from pathlib import Path
from sys import platform

import numpy as np
import pandas as pd
import yaml

from src.config import SRC


def get_names_dataset(path=SRC / "original_data"):
    """This function extract the name of the data set in "origina_data" folder
    Args:
        path (str, path object): The path to the folder where original data is stored.
        Default value is SRC/"original_data".
    Returns:
        name (list): the list containing the names of dataset.
    """
    if platform == "win32":  # here differantia between operating system
        a = r"\*"
        b = "\\"
    elif platform == "darwin":
        a = r"/*"
        b = "/"

    files = list(glob.glob(str(path) + f"{a}"))
    name = []
    for i in range(len(files)):
        name.append(files[i].split(f"{b}")[-1].split("_")[0])
    return name


def clean_data(df, i, negatives=None):
    """This function does the bacis cleaning. It renames the columns of the dataset based
        on the csv file provided and replaces the negative values in the dataset with NaN.
    Args:
        df (pandas.DataFrame): The dataframe to be cleaned.
        i (str): The name of the dataset.
        negatives(list) : List of negative integars which is replaced by NaN. Default values are
        all negative integars from -1 to -11.
    Returns:
        df (pandas.DataFrame): The dataframe with new column names and without negative values.
    """
    if negatives is None:
        negatives = list(range(-1, -11, -1))

    new_names = pd.read_csv(SRC / f"data_management/{i}/{i}_renaming.csv", sep=";")[
        "new_name"
    ]  # reading the new column names form csv file
    renaming_dict = dict(zip(df.columns, new_names))
    renaming_dict = {
        k: v for k, v in renaming_dict.items() if pd.notna(v)
    }  # Deleting NA values (column names whose new name is not given in the csv file)
    df = df.rename(columns=renaming_dict)
    for i in negatives:
        df[df == i] = np.nan
    return df


def reverse_code(df):
    """This function reversed the values of the pre-determined variables.
    Those values are determined in csv file
    by adding "_neg" to their new name
    Args:
        df (pandas.DataFrame): The dataframe whose variables is going to be reversed
    Returns:
        df (pandas.DataFrame): The dataframe with new reversed variables.
    """
    for i in df:
        if (f"{i}" in df.filter(regex="_neg")) is True:
            new_column_name = i.replace("_neg", "")
            value_to_be_used = df[i].max() + 1
            df[new_column_name] = value_to_be_used - df[i]
    return df


def average_big5(df):
    """This function aggregates the variable for big5 classification
     by taking their mean for each observation
     Args:
        df (pandas.DataFrame): The dataframe which has big5 variables (PENDDAT)
    Returns:
        df (pandas.DataFrame): The dataframe with new aggreagted
         big5 variables."""
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
    """This function aggregates the variable for traditional gender role
     by taking their mean for each observation
     Args:
        df (pandas.DataFrame): The dataframe which has traditional
         gender role variables (PENDDAT)
    Returns:
        df (pandas.DataFrame): The dataframe with new aggreagted
         traditional gender role variable."""
    df["genrole_traditional"] = df.filter(regex="traditional").mean(axis=1)
    return df


def create_dummies(df_p, df_h, dummies_p=None, dummies_h=None):
    """This function creates dummies for the variables provided in a .yaml file.
    It preserves the original values and
    adds another column to dataset with name {variable_name}_dummy.
    This function creates dummies for variable which has
    two possible answer (e.g. Yes=1,No=2).This variables are coded as "others" in .yaml file.
    Args:
        df_p (pandas.DataFrame): Personal dataset(PENDDAT) to create dummies
        df_h (pandas.DataFrame): Household dataset(HHENDDAT) to create dummies
        dummies_p (str,path object) : Path to the .yaml file containing the list of variables
        from Personal dataset (PENDDAT) whose dummies are going to be created
        dummies_h (str,path object) : Path to the .yaml file containing the list of variables
        from Households dataset (HHENDDAT) whose dummies are going to be created
    Returns:
        df_p (pandas.DataFrame): Personal dataset(PENDDAT) with dummy variables
        df_h (pandas.DataFrame): Household dataset(HHENDDAT) with dummy variables
    """
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
    """This function creates dummies for the deprivation variables provided in a .yaml file.
    It preserves the original values and
    adds another column to dataset with name {variable_name}_dummy.
    This variables are coded as "deprivation" in .yaml file.
    Only household dataset contains deprivation variables.
    Args:
        df_h (pandas.DataFrame): Household dataset(HHENDDAT) to create dummies
        dummies_h (str,path object or file-like object) : Path to the .yaml file
        containing the list of deprivation variables
        from Households dataset (HHENDDAT) whose dummies are going to be created
    Returns:
        df_h (pandas.DataFrame): Household dataset(HHENDDAT) with dummy variables
    """
    if dummies_h is None:
        dummies_h = Path(SRC / "data_management/dummies/HHENDDAT_dummies.yaml")
    with open(dummies_h) as stream:
        dummies_h = yaml.safe_load(stream)
    for dummies in dummies_h["deprivation"]:
        df_h.loc[df_h[f"{dummies}b"] == 1, f"{dummies}_dummy"] = 1
        df_h.loc[df_h[f"{dummies}b"] == 2, f"{dummies}_dummy"] = 0
        df_h.loc[df_h[f"{dummies}a"] == 1, f"{dummies}_dummy"] = 0
    return df_h
