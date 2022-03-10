from pathlib import Path

import pandas as pd
import pytask
import yaml

from src.config import BLD
from src.config import SRC


@pytask.mark.depends_on(
    {
        "first": BLD / "weighted_data" / "HHENDDAT_weighted.pickle",
        "second": BLD / "weighted_data" / "PENDDAT_weighted.pickle",
    }
)
@pytask.mark.produces(SRC / "paper")
def task_creating_summary_stat_tex(depends_on, produces):

    df_h = pd.read_pickle(depends_on["first"])
    df_p = pd.read_pickle(depends_on["second"])
    with open(SRC / r"final\PENDDAT_stat.yaml") as stream:
        dict_stat_p = yaml.safe_load(stream)
    with open(SRC / r"final\HHENDDAT_stat.yaml") as stream:
        dict_stat_h = yaml.safe_load(stream)
    data = []
    for x, _y in dict_stat_p.items():
        data.append(df_p.reset_index()[f"{x}"])
        df_p_stat = pd.DataFrame(data)
        df_p_stat = df_p_stat.rename(index=dict_stat_p).T.describe()
        df_p_stat = (
            df_p_stat.T.drop(["25%", "75%", "count"], axis=1)
            .rename(columns={"50%": "median"})
            .round(2)
        )
    df_p_stat.to_latex(str(Path(produces)) + "/PENDDAT_sum_stat.tex")
    data = []
    for x, _y in dict_stat_h.items():
        data.append(df_h.reset_index()[f"{x}"])
        df_h_stat = pd.DataFrame(data)
        df_h_stat = df_h_stat.rename(index=dict_stat_h).T.describe()
        df_h_stat = (
            df_h_stat.T.drop(["25%", "75%", "count"], axis=1)
            .rename(columns={"50%": "median"})
            .round(2)
        )
    df_h_stat.to_latex(str(Path(produces)) + "/HHENDDAT_sum_stat.tex")


r"""     names = ["PENDDAT", "HHENDDAT"]

    for i in names:
        df = pd.read_pickle(str(Path(depends_on)) + f"/{i}_weighted.pickle")

        with open(SRC / fr"final\{i}_stat.yaml") as stream:
            dict_stat = yaml.safe_load(stream)

        data = []
        for x, _y in dict_stat.items():
            data.append(df.reset_index()[f"{x}"])
        df_stat = pd.DataFrame(data)
        df_stat = df_stat.rename(index=dict_stat).T.describe()
        df_stat = (
            df_stat.T.drop(["25%", "75%", "count"], axis=1)
            .rename(columns={"50%": "median"})
            .round(2)
        )
        df_stat.to_latex(str(Path(produces)) + f"/{i}_sum_stat.tex")
        """
