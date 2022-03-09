"""
This file contains general cleaning functions for renaming of variables
and replacing of values.

These functions should be general enough to be shared across
liss-data and soep cleaning repos.
"""
import datetime
import math
import warnings

import numpy as np
import pandas as pd
from pandas.api.types import infer_dtype


def replace_values(panel, replace_dict, rename_df):
    """Replace and rename values using the replace dictionary.

    Args:
        panel (pandas.DataFrame): The dataframe which values need to be
            replaced or renamed.
        replace_dict (dictionary): The replacing dictionary.
        rename_df (pandas.DataFrame): The renaming dataframe taken from the
            renaming file.

    Returns:
        pandas.DataFrame: The dataframe with the replaced or renamed values.

    """

    out = panel.copy()
    # Convert some columns to lower case
    if "mixed_case" in replace_dict and replace_dict["mixed_case"]:
        out[replace_dict["mixed_case"]] = out[replace_dict["mixed_case"]].apply(
            lambda x: x.str.lower()
        )

    # Convert numeric columns
    if "numeric" in replace_dict and replace_dict["numeric"]:
        out[replace_dict["numeric"]] = out[replace_dict["numeric"]].apply(
            lambda x: pd.to_numeric(x, errors="coerce")
        )

    # Rename variables according to their types.
    if "type renaming" in replace_dict:

        rename_df["type"] = rename_df["type"].replace(
            {
                "int": "Int64",
                "float": "float64",
                "bool": "boolean",
                "Categorical": "category",
                "Int": "Int64",
            }
        )

        for group in replace_dict["type renaming"] and replace_dict["type renaming"]:
            filter = rename_df["type"] == group
            cols = rename_df.copy()[filter]["new_name"].values
            for col in cols:
                try:
                    out[col] = out[col].replace(replace_dict["type renaming"][group])
                except Exception:
                    print(f"issue with {col}")
                    continue

    # Rename variables in multiple columns.
    if "multicolumn" in replace_dict and replace_dict["multicolumn"]:
        for _j in replace_dict["multicolumn"]:
            try:
                out.loc[:, replace_dict["multicolumn"][_j]["columns"]] = out.loc[
                    :, replace_dict["multicolumn"][_j]["columns"]
                ].replace(replace_dict["multicolumn"][_j]["dictionary"])
            except Exception:
                print(f"error in {replace_dict['multicolumn'][_j]}")

    # Rename variables according to the renaming dictionary
    if "replacing" in replace_dict and replace_dict["replacing"]:
        for _i in replace_dict["replacing"]:
            if _i != "full_df":
                try:
                    out[_i].replace(replace_dict["replacing"][_i], inplace=True)
                except TypeError:
                    print(f"type issue with {_i}")
            else:
                try:
                    out.replace(replace_dict["replacing"][_i], inplace=True)
                except TypeError:
                    print(f"type issue with {_i}")

    out = _check_for_two_types_of_missing(out)

    return out


def logical_cleaning(panel, logical_cleaning_dict):
    """Some logical cleaning specified in logical_cleaning_dict.

    Args:
        panel (pandas.DataFrame): The dataframe which values need to be
            replaced or renamed.
        logical_cleaning_dict (dictionary): The specificaiton dictionary.


    Returns:
        pandas.DataFrame: The dataframe with the replaced or renamed values.

    """
    out = panel.copy()

    # Fill nans
    if "fillna" in logical_cleaning_dict and logical_cleaning_dict["fillna"]:
        for col, value in logical_cleaning_dict["fillna"].items():
            print(col, out[col].dtype)
            out[col] = out[col].fillna(value)

    return out


def set_types_file(
    panel,
    rename_df,
    cat_sep=", ",
    int_to_float=True,
    bool_to_float=True,
    num_str_categorical=18,
    scale_as_category=True,
):
    """Assign types to the columns in `panel` using the renaming file.

    Args:
        panel (pandas.DataFrame): The dataframe which types need to be
            assigned.
        rename_df (pandas.DataFrame): The renaming dataframe taken from the
            renaming file.
        cat_sep (string): The separator of the categories in the file.
        int_to_float (boolean): True if values specified as int should be coded
            as float.
        bool_to_float (boolean): True if values specified as boolean should be
            coded as float.
        num_str_categorical (int): Number of unique values for inferance as
            category

    Returns:
        pandas.DataFrame: The dataframe with the new types assigned.

    """
    out = panel.copy()

    if "type" in rename_df.columns:
        out = _set_types_file_with_file(
            panel=out,
            rename_df=rename_df,
            cat_sep=cat_sep,
            int_to_float=int_to_float,
            bool_to_float=bool_to_float,
            num_str_categorical=num_str_categorical,
            scale_as_category=scale_as_category,
        )
    else:
        for var in out.columns.values:
            out.loc[:, var] = _set_inferred_types(
                out[var],
                int_to_float=int_to_float,
                bool_to_float=bool_to_float,
                num_str_categorical=num_str_categorical,
            )
    for i in ["personal_id", "year"]:
        if i in out:
            out[i] = out[i].astype("int")

    return out


def _set_inferred_types(
    col, num_str_categorical=18, int_to_float=False, bool_to_float=False
):
    out_col = col.copy()
    inf_type = infer_dtype(out_col, skipna=True)
    expected_type = None
    if inf_type in ("string", "mixed-integer", "mixed"):
        try:
            out_col = pd.to_numeric(out_col)
            inf_type = infer_dtype(out_col)
        except Exception:
            if len(col.unique()) <= num_str_categorical:
                expected_type = "category"
            else:
                expected_type = "object"

    if inf_type in ("floating", "integer", "mixed-integer-float"):
        out_col = out_col.map(lambda x: np.nan if pd.isnull(x) else x)

        if [x for x in (out_col % np.floor(out_col)).unique() if not pd.isnull(x)] == [
            0
        ] or np.all(out_col.dropna().unique() == [0]):

            if int_to_float:
                try:
                    out_col = out_col.astype("float64")
                except Exception:
                    warnings.warn(
                        f"{out_col.name} cannot be converted to inferred type.",
                        UserWarning,
                    )
            else:

                try:
                    out_col = out_col.astype("Int64")
                except Exception:
                    warnings.warn(
                        f"{out_col.name} cannot be converted to inferred type.",
                        UserWarning,
                    )

            if all(x in range(11) for x in out_col.dropna()) or all(
                x in range(1990, 2025) for x in out_col.dropna()
            ):
                expected_type = "category"
            else:
                expected_type = "float64" if int_to_float else "Int64"
        else:
            expected_type = "float64"

    if inf_type == "boolean":
        expected_type = "float64" if bool_to_float else inf_type
    if inf_type == "categorical":
        expected_type = "category"

    if pd.isnull(expected_type) or inf_type in ("mixed"):
        warnings.warn(
            f"{out_col.name} contains bad combination of values! Either"
            " further cleaning is required or an error occured",
            UserWarning,
        )
        out_col = out_col.astype("object")
    else:
        # HOTFIX: Debug whole fucntion!
        try:
            out_col = out_col.astype(expected_type)
        except Exception:
            warnings.warn(
                f"{out_col.name} cannot be converted to inferred type.",
                UserWarning,
            )
    return out_col


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


def _set_types_file_with_file(
    panel,
    rename_df,
    cat_sep=", ",
    int_to_float=True,
    bool_to_float=True,
    num_str_categorical=18,
    scale_as_category=True,
):
    """Assign types to the columns in `panel` using the renaming file.

    Args:
        panel (pandas.DataFrame): The dataframe which types need to be
            assigned.
        rename_df (pandas.DataFrame): The renaming dataframe taken from the
            renaming file.
        cat_sep(string): The separator of the categories in the file.
        int_to_float(boolean): True if values specified as int should be coded
            as float.
        bool_to_float(boolean): True if values specified as boolean should be
            coded as float.
        num_str_categorical(int): Number of unique values for inferance as
            category.
        scale_as_category(boolean): True if values specified as scales should
            be catagories.

    Returns:
        pandas.DataFrame: The dataframe with the new types assigned.

    """
    out = panel.copy()

    rename_df = rename_df.set_index("new_name")
    rename_df["type"] = rename_df["type"].replace(
        {
            "int": "Int64",
            "float": "float64",
            "bool": "boolean",
            "Categorical": "category",
            "Int": "Int64",
        }
    )

    if scale_as_category:
        rename_df["type"] = rename_df["type"].replace({"scale": "category"})
    if int_to_float:
        rename_df["type"] = rename_df["type"].replace({"Int64": "float64"})
    if bool_to_float:
        rename_df["type"] = rename_df["type"].replace({"boolean": "float64"})

    for var in out.columns.values:
        try:
            expected_type = rename_df.loc[var, "type"]
        except KeyError:
            continue

        if expected_type == expected_type:

            if expected_type == "scale":
                cats_scale = rename_df.loc[var, "categories"].split(cat_sep)
                replace_dict = dict(zip(cats_scale, np.arange(0, len(cats_scale))))
                try:
                    out[var].replace(replace_dict, inplace=True)
                except TypeError:
                    try:
                        cats_scale = [int(s) for s in cats_scale]
                        replace_dict = dict(
                            zip(cats_scale, np.arange(0, len(cats_scale)))
                        )
                        out[var].replace(replace_dict, inplace=True)
                    except Exception:
                        print(f"Problem converting to scale var: {var}")
                if int_to_float:
                    expected_type = "float64"
                else:
                    expected_type = "Int64"

            try:
                out[var] = out[var].astype(expected_type)
            except TypeError:
                print(f"could not convert {var} to {expected_type}")
            except Exception:
                print(
                    f"unexpected error converting the type of {var} to {expected_type}"
                )

            if (
                expected_type == "category"
                and rename_df.loc[var, "categories"] == rename_df.loc[var, "categories"]
            ):
                try:
                    cats = [
                        int(s) for s in rename_df.loc[var, "categories"].split(cat_sep)
                    ]
                except Exception:
                    cats = rename_df.loc[var, "categories"].split(cat_sep)
                if rename_df.loc[var, "ordered"] == rename_df.loc[var, "ordered"]:
                    try:
                        out[var].cat.set_categories(
                            cats,
                            ordered=rename_df.loc[var, "ordered"],
                            inplace=True,
                        )
                    except Exception:
                        warnings.warn(
                            f"for {out[var]} there is an error in the "
                            "categories specified in the file",
                            UserWarning,
                        )

                else:
                    try:
                        out[var].cat.set_categories(cats, inplace=True)
                    except Exception:
                        warnings.warn(
                            f"for {out[var]} there is an error in the "
                            "categories specified in the file",
                            UserWarning,
                        )
        else:
            out.loc[:, var] = _set_inferred_types(
                out[var],
                int_to_float=int_to_float,
                bool_to_float=bool_to_float,
                num_str_categorical=num_str_categorical,
            )

    return out


def convert_time_cols(panel, cols):
    """Convert the observations in the selected time columns of a panel to a
    HH:MM:SS format. If they are in seconds from the beginning of the day.

    Args:
        panel(pandas.DataFrame): The data frame to be converted.
        cols(list): The time columns to be converted.

    Returns:
        pandas.DataFrame: the data frame with the time in the format specified.
    """
    out = panel.copy()
    for col in cols:
        out[col] = out[col].apply(
            lambda x: str(datetime.timedelta(seconds=math.floor(x)))
            if type(x) == float and x == x
            else x
        )
    return out


# def check_description(data, data_name, description, logging="print"):
#     """Perform various checks on the description table.
#     Args:
#         data (pd.DataFrame): DataFrame with the survey data.
#         data_name (str): column name of the raw variable names
#         description (pd.DataFrame): DataFrame describing the finished DataFrame.
#         logging (str, optional): Path to a text file, "print" or None
#     """
#     _check_new_names(new_names=description["new_name"], logging=logging)
#     _check_types(types=description["type"], logging=logging)
#     _check_var_overlap_btw_description_and_data(
#         data_vars=data.columns, covered=description[data_name].unique(),
# logging=logging
#     )
#     labels = description.set_index("new_name")["label_english"]
#     if labels.isnull().any():
#         msg = "The following variables don't have a label:\n\t" + "\n\t".join(
#             labels[labels.isnull()].index
#         )
#         _custom_logging(msg=msg, logging=logging)

#     # warn if topic missing


# def _check_new_names(new_names, logging):
#     if new_names.duplicated().any():
#         msg = f"{new_names[new_names.duplicated()]} are duplicates in
# the new_name column."
#         _custom_logging(msg=msg, logging=logging)
#     if new_names.isnull().any():
#         msg = "The new name column should not contain NaNs"
#         _custom_logging(msg=msg, logging=logging)

#     lengths = new_names.str.len()
#     too_long = new_names[lengths > 31]
#     if len(too_long) > 0:
#         msg = "The following new names are too long for STATA:\n\t" + "\n\t".join(
#             too_long
#         )
#         _custom_logging(msg=msg, logging=logging)


# def _check_types(types, logging):
#     if types.isnull().any():
#         msg = "You should declare a type for every variable."
#         _custom_logging(msg=msg, logging=logging)

#     known_types = ["boolean", "Int64", "float", "Categorical", "str"]
#     if not types.isin(known_types).all():
#         unknown_types = types[~types.isin(known_types)].tolist()
#         msg = "There are unknokn types: \n" + "\n\t".join(unknown_types)
#         _custom_logging(msg=msg, logging=logging)


# def _check_var_overlap_btw_description_and_data(data_vars, covered, logging):
#     missing_in_description = [x for x in data_vars if x not in covered]
#     if missing_in_description:
#         msg = (
#             "The following variables from the raw dataset are not "
#             + "covered by the description table: \n\t"
#             + "\n\t".join(sorted(missing_in_description))
#         )
#         _custom_logging(msg=msg, logging=logging)
#     missing_in_data = [str(x) for x in covered if x not in data_vars]
#     if missing_in_data:
#         msg = (
#             "The following variables from the description table are not "
#             + "in the raw dataset: \n\t"
#             + "\n\t".join(sorted(missing_in_data))
#         )
#         _custom_logging(msg=msg, logging=logging)


# def _check_categorical_cols(description, data_name, logging):
#     cat_df = description[description["type"] == "Categorical"]
#     no_categories = cat_df["categories"].isnull()
#     if no_categories.any():
#         msg = "English category labels are missing for: \n\t" + "\n\t".join(
#             cat_df[no_categories]["new_name"]
#         )
#         _custom_logging(msg=msg, logging=logging)
#     ordered_bool = cat_df["ordered"].isin([True, False])
#     if not ordered_bool.all():
#         msg = "The ordered column must be boolean but is not for: \n\t" + "\n\t".join(
#             cat_df[~ordered_bool]["new_name"]
#         )
#         _custom_logging(msg=msg, logging=logging)


# def _custom_logging(msg, logging):
#     """Print or write msg to a file."""
#     padded = "\n\n" + msg + "\n\n"
#     if logging == "print":
#         print(padded)
#     elif logging is not None:
#         with open(logging, "a") as f:
#             f.write(padded)


# def convert_dtypes(df, num_str_categorigal=18):
#     """
#     This function assigns new dtypes accroding to the follwing logic:
#     - If the col contains no float and no str-float and has less than 20 values
#       it is coverted to category otherwise left at object!
#     - If the col contains only float or str float in either range(11)
# or range(1990,2025)
#       it is converted to categorical numeric
#     - If the col only contains floats it is converted to numeric!
#     In other cases a warning is raised! Since we do not want these cases to happen!

#     """
#     out = df.copy()
#     for col in out:
#         if out[col].dtype.name == "object":
#             contains_float = _check_for_float(out[col].unique())
#             contains_string_float = _check_for_str_number(out[col].unique())

#             # Convert string only cols to cat if they have few labels
#             if (
#                   (contains_string_float is False) and
#                   (len(out[col].unique()) <= num_str_categorigal)):
#                 out[col] = out[col].astype("category")

#             # Convert cols to cat that contain only floats/str in range(11)
#             elif contains_string_float in ["categorical", "years"]:
#                 out[col] = out[col].astype("float").astype("category")

#             # Convert float only strings to numerical
#             elif contains_float == "all":
#                 out[col] = out[col].astype("float")

#             # Print the rest and maybe inlcude warning for weird cols
#             elif contains_float == "some" or contains_string_float == "some":
#                 warnings.warn(
#                     f"{col} contains bad combination of values! Either"
#                     "further cleaning is required or an error occured",
#                     UserWarning,
#                 )

#     return out


# def _check_for_str_number(values):
#     """
#     Check for str floats in list of values
#     """
#     out = []
#     values = [x for x in values if str(x) != "nan"]

#     for x in values:
#         try:
#             out.append(float(x))
#         except ValueError:
#             continue
#     if len(out) == len(values) and all(x in range(11) for x in out):
#         return "categorical"
#     elif len(out) == len(values) and all(x in range(1990, 2025) for x in out):
#         return "years"
#     elif len(out) > 0:
#         return "some floats/str numbers"
#     else:
#         return False


# def _check_for_float(values):
#     """
#     Check for presence of floats in list
#     """
#     values = [x for x in values if str(x) != "nan"]
#     if all(type(x) == float for x in values):
#         return "all"
#     elif any(type(x) == float and x != np.nan for x in values):
#         return "some"
#     else:
#         return False
