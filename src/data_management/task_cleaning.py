import pandas as pd
import pytask

from src.config import BLD
from src.config import SRC
from src.data_management.cleaning_functions import *

names = get_names_dataset()


@pytask.mark.parametrize(
    "depends_on, produces,i",
    [
        (
            SRC / f"original_data/{i}_cf_W11.dta",
            BLD / "cleaned_data" / f"{i}_clean.pickle",
            i,
        )
        for i in names
    ],
)
def task_basic_cleaning(depends_on, produces, i):
    """This task does the basic cleaning for the dataset provided.
    It loads dataset from the folder called "original_data,
    then renames the columns based on csv file as well as replaces
    negative values with NaN. Then it saves the datasets
    into "BLD/cleaned_data"
    """
    df = pd.read_stata(depends_on, convert_categoricals=False)
    if "pnr" in df.columns:
        if "hnr" in df.columns:
            df = clean_data(df, i).set_index(["hh_id", "wave", "p_id"]).sort_index()
        else:
            df = clean_data(df, i).set_index(["wave", "p_id"]).sort_index()
    else:
        df = clean_data(df, i).set_index(["hh_id", "wave"]).sort_index()
    df.to_pickle(produces)


@pytask.mark.depends_on(
    {
        "first": BLD / "cleaned_data" / "PENDDAT_clean.pickle",
        "second": BLD / "cleaned_data" / "HHENDDAT_clean.pickle",
    }
)
@pytask.mark.produces(
    {
        "first": BLD / "aggregated_data" / "PENDDAT_aggregated.pickle",
        "second": BLD / "aggregated_data" / "HHENDDAT_aggregated.pickle",
    }
)
def task_aggregation_and_dummy(depends_on, produces):
    """This task does the aggregation (creating variables for traditional gender roles,
     big5 and reversing the variables)
    and creating dummy variables for the dataset provided (PENDDAT and HHENDDAT).
    It loads the cleaned dataset from the
    folder called "BLD/cleaned_data".
    Then it saves the datasets into "BLD/aggregated_data"
    """
    df_p = pd.read_pickle(depends_on["first"])
    df_h = pd.read_pickle(depends_on["second"])

    df_p = reverse_code(df_p)  # reverse all the negatively phrased variables
    df_p = average_big5(df_p)  # get facet averages for big five
    df_p = average_eri(df_p)  # get facet averages for eri
    df_p = average_genrole(df_p)  # get traditional gender role average

    df_p, df_h = create_dummies(df_p, df_h)
    df_h = create_dummies_depr(df_h)
    df_p.to_pickle(produces["first"])
    df_h.to_pickle(produces["second"])


@pytask.mark.depends_on(
    {
        "first": BLD / "cleaned_data" / "hweights_clean.pickle",
        "second": BLD / "cleaned_data" / "pweights_clean.pickle",
        "third": BLD / "aggregated_data" / "HHENDDAT_aggregated.pickle",
        "fourth": BLD / "aggregated_data" / "PENDDAT_aggregated.pickle",
    }
)
@pytask.mark.produces(
    {
        "first": BLD / "final_data" / "merged_clean.pickle",
        "second": BLD / "weighted_data" / "HHENDDAT_weighted.pickle",
        "third": BLD / "weighted_data" / "PENDDAT_weighted.pickle",
    }
)
def task_merging(depends_on, produces):
    """This task merges the dataset. It first merges personal
    and household datasets with their weights. In addition,
    It also merges household and personal datasets
    """
    df_h_c = pd.read_pickle(depends_on["third"])
    df_p_c = pd.read_pickle(depends_on["fourth"])
    df_h_w = pd.read_pickle(depends_on["first"])
    df_p_w = pd.read_pickle(depends_on["second"])

    merged_h = pd.merge(df_h_c, df_h_w, on=["wave", "hh_id"], how="left")
    merged_p = pd.merge(
        df_p_c.reset_index(), df_p_w, on=["wave", "p_id"], how="left"
    ).set_index(["hh_id", "wave", "p_id"])
    merged_w = pd.merge(
        merged_p.reset_index(),
        merged_h,
        on=["wave", "hh_id", "survey_year", "survey_mon"],
        how="outer",
        indicator=True,
    ).set_index(["wave", "hh_id", "p_id"])
    merged_w.to_pickle(produces["first"])
    merged_h.to_pickle(produces["second"])
    merged_p.to_pickle(produces["third"])

    # os.remove(BLD / "cleaned_data" / "HHENDDAT_clean.pickle")
    # os.remove(BLD / "cleaned_data" / "PENDDAT_clean.pickle")
    # os.remove(BLD / "cleaned_data" / "hweights_clean.pickle")
    # os.remove(BLD / "cleaned_data" / "pweights_clean.pickle")
    # os.remove(BLD / "aggregated_data" / "PENDDAT_aggregated.pickle")
    # os.remove(BLD / "aggregated_data" / "HHENDDAT_aggregated.pickle")
