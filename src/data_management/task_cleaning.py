import pandas as pd
import pytask

from src.config import BLD
from src.config import SRC


def p_renaming_columns(path):
    df = pd.read_stata(path, convert_categoricals=False)  # read the dataset from path
    new_names = pd.read_csv(
        SRC / "data_management/PENDDAT/penddat_renaming.csv", sep=";"
    )["new_name"]
    renaming_dict = dict(zip(df.columns, new_names))
    renaming_dict = {
        k: v for k, v in renaming_dict.items() if pd.notna(v)
    }  # Deleting NA values
    return df.rename(columns=renaming_dict)


@pytask.mark.depends_on(SRC / "original_data/PENDDAT_cf_W11.dta")
@pytask.mark.produces(BLD / "p_clean.dta")
def task_clean_penddat(depends_on, produces):
    df = p_renaming_columns(depends_on)
    df.to_stata(produces)
