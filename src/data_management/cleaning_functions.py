import numpy as np
import pandas as pd

from src.config import SRC


def replacing_negative(data_set, negatives=None):
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
