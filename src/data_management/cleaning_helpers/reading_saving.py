"""
This file contains some functions for the reading and
saving of files
"""
import os
import warnings

import numpy as np
import pandas as pd
import yaml


def read_stata(
    file_path,
    convert_categoricals,
    vars_to_keep,
    start_year=None,
    renaming_complete=False,
):
    """Emulate pd.read_stata() that takes care of duplicate entries for
    convert_categoricals=True.

    Args:
        file_path (string): path to file
        convert_categoricals (boolean): Read value labels and convert columns
                                        to Categorical/Factor variables.
        vars_to_keep (list): columns that should be kept
        start_year: all observations before this year are dropped


    Returns:
        DataFrame: loaded file
    """

    data = pd.read_stata(file_path, convert_categoricals=False)
    missing_from_data = [x for x in vars_to_keep if x not in data]
    missing_from_rename = [x for x in data.columns if x not in vars_to_keep]

    # Raise error if expected variable not in data
    if len(missing_from_data) > 0:
        warnings.warn(
            f"Problem with {file_path}: the following variables "
            "are specified in the renaming file, but not in the "
            "dataset:\n\t" + "\n\t".join(missing_from_data)
        )

    # Raise error if expected variable not in renaming file
    if len(missing_from_rename) > 0 and renaming_complete:
        warnings.warn(
            f"Problem with {file_path}: the following "
            "variables are not in the renaming table and as a consequence "
            "dropped:\n\t" + "\n\t".join(missing_from_rename)
        )

    # Select only subset of variables
    data = data[[c for c in data if c in vars_to_keep]]

    if start_year:
        raise NotImplementedError

    if convert_categoricals:

        # More general version to convert needed than the built-in one.
        value_label_dict = pd.io.stata.StataReader(file_path).value_labels()
        for col in data:
            if col in value_label_dict:
                cat_data = pd.Categorical(data[col], ordered=True)
                categories = []
                for category in cat_data.categories:
                    if category in value_label_dict[col]:
                        categories.append(value_label_dict[col][category])
                    else:
                        categories.append(category)  # Partially labeled
                if len(categories) == len(set(categories)):
                    cat_data.categories = categories
                    data[col] = cat_data
    return data


def load_general_specs(spec_path, data_set_name):
    """Load general specifications for one data set

    Args:
        data_set_name (string): name of data set
        spec_path (string): path to specification folder

    Returns:
        dictionary: general specifications
    """
    with open(spec_path / "data_sets_specs.yaml") as fn:
        specs = yaml.load(fn, Loader=yaml.FullLoader)[data_set_name]

    return specs


def load_rename_df(spec_path, data_set_name):
    rename_df = pd.read_csv(spec_path / f"{data_set_name}_renaming.csv", sep=";")
    rename_df = rename_df.dropna(subset=["new_name"])

    # Check validity of rename df
    check_new_name_in_renaming(
        rename_df, data_set_name, suffixes_exception=["_merge", "_202002", "_update"]
    )
    return rename_df


def removesuffix(complete_str, suffix):
    # ToDo: when only Python versions after 3.9 are supported, can
    # ToDo: just use the method of the standard library
    if suffix and complete_str.endswith(suffix):
        return complete_str[: -len(suffix)]
    else:
        return complete_str[:]


def check_new_name_in_renaming(rename_df, data_set_name, suffixes_exception=None):
    # ToDo: write unit tests for this function

    rename_df = rename_df.copy()
    if suffixes_exception:
        for suf in suffixes_exception:
            rename_df["new_name"] = rename_df["new_name"].apply(
                lambda x: removesuffix(x, suf)
            )

    # Raise error if too long!
    too_long = rename_df["new_name"].str.match("^.{32,}")
    if too_long.any():

        too_long_variable_names = rename_df.loc[too_long, "new_name"].values
        raise (
            ValueError(
                "column names are too long. Please shorten the variable",
                " names to a maximum of 32 letters"
                f" {too_long_variable_names} in the renaming file for {data_set_name}",
            )
        )


def load_specs(data_set_name, spec_path, start_year=None):
    """specification and renaming file

    Args:
        data_set_name (string): name of data set
        spec_path (string): path to general specification folder

    Returns:
        dict: specifications for data set
        DataFrame: renaming specifications
        dict: replacing spefications
    """

    # Load specifications
    specs = load_general_specs(spec_path, data_set_name)

    # Load rename df
    rename_df = load_rename_df(spec_path / data_set_name, data_set_name)

    # Load further cleaning spec files if they exist
    cleaning_specs = {}
    for cleaning_type in ["replacing", "logical_cleaning"]:
        replace_path = (
            spec_path / data_set_name / f"{data_set_name}_{cleaning_type}.yaml"
        )
        if os.path.isfile(replace_path):
            with open(replace_path) as file:
                cleaning_specs[f"{cleaning_type}"] = yaml.load(
                    file, Loader=yaml.FullLoader
                )

    return specs, rename_df, cleaning_specs


def variable_cleaning_for_dta(panel):
    """Clean variables such that they can be exportet to Stata."""
    # Change dtype of object variables
    type_pref = [int, float, str]
    for colname in list(panel.select_dtypes(include=["object"]).columns):
        for t in type_pref:
            try:
                panel[colname] = panel[colname].astype(t)
            except (ValueError, TypeError):
                pass

    for x in panel.dtypes.index:
        if panel.dtypes[x].name == "object":
            panel = panel.drop(columns=[x])

    # retype new boolean and integer types
    for x in panel.dtypes.index:
        if panel.dtypes[x].name in ["boolean", "Int64"]:
            panel[x] = panel[x].astype("float")

    # Replace characters that cannot be read by Stata
    panel = panel.replace(["€"], ["EUR"], regex=True)
    panel = panel.replace(["\u2019"], ["'"], regex=True)
    panel = panel.replace(["nan"], [""], regex=True)
    panel = panel.replace([None], [""], regex=True)

    return panel


def save_frame(panel, out_path, vars_to_keep=None):
    """Check file, determine file type and save file

    Args:
        vars_to_keep (list): List of variables to keep

    """
    if vars_to_keep is None:
        vars_to_keep = []
    # Restrict to variables to keep
    if len(vars_to_keep) > 0:
        missing_from_data = [x for x in vars_to_keep if x not in panel]
        if len(missing_from_data) > 0:
            warnings.warn(
                f"Problem with {out_path}: the following variables "
                "are specified as vars to keep, but are not in the "
                "dataset:\n\t" + "\n\t".join(missing_from_data)
            )
        panel = panel[[c for c in vars_to_keep if c not in missing_from_data]]

    # Check file
    empty_columns = panel.isnull().all()
    if empty_columns.any():
        empty_names = empty_columns.index[empty_columns]
        formatted_names = ", ".join(empty_names)
        raise ValueError(f"Column(s) {formatted_names} consist(s) only of NAs.")

    panel = _check_for_duplicated_cols_or_indices(panel)
    panel = _check_for_two_types_of_missing(panel)

    # Determine file extension
    out_format = out_path.suffix[1:]

    # Save always as pickle
    panel.to_pickle(out_path)

    # Save file in out format
    if out_format == "dta":
        try:
            variable_cleaning_for_dta(panel).to_stata(out_path)
        except (KeyboardInterrupt, SystemExit):
            raise
        except ValueError:
            variable_cleaning_for_dta(panel).to_stata(out_path, version=117)
    elif out_format == "csv":
        panel.to_csv(out_path, sep=";")
    elif out_format == "parquet":
        panel.to_parquet(out_path)
    elif out_format == "pickle":
        pass
    else:
        raise ValueError('"format" must be one of pickle, dta, csv, parquet')


def load_frame(in_path):
    """Determine file type and save file"""

    # Determine file extension
    out_format = in_path.suffix[1:]

    # Save file
    if out_format == "pickle":
        out = pd.read_pickle(in_path)
    elif out_format == "dta":
        out = pd.read_stata(in_path)
    elif out_format == "csv":
        out = pd.read_csv(in_path, sep=";")
    elif out_format == "parquet":
        out = pd.read_parquet(in_path)
    else:
        raise ValueError('"format" must be one of pickle, dta, csv, parquet')

    return out


def _check_for_duplicated_cols_or_indices(df):
    """Check for duplicated column names or indices"""

    # Columns
    if any(df.columns.duplicated()):
        duplicated = list(df.columns[df.columns.duplicated()])
        raise KeyError(f"The following column names are non-unique: {duplicated}")

    # Indices
    if any(df.index.duplicated()):
        duplicated = list(df.index[df.index.duplicated()])
        raise KeyError(f"The following indices are non-unique: {duplicated}")

    return df


def _check_for_two_types_of_missing(df):
    # Make sure columns contain only one type of missing
    for col in df.columns:
        pd_na = any(x is pd.NA for x in list(df[col].unique()))
        np_na = any(x is np.nan for x in list(df[col].unique()))
        if pd_na & np_na:
            print(
                f"{col} contains two types of nans. Will be fixed. Should be checked!"
            )
            df[col] = df[col].map(lambda x: _fix_nans(x))
    return df


def _fix_nans(x):
    if pd.isna(x):
        return np.nan
    else:
        return x
