r""" from pathlib import Path

import pandas as pd
import pytask
import yaml

from src.config import BLD
from src.config import SRC


@pytask.mark.depends_on({
        "first": BLD / "weighted_data" / "HHENDDAT_weighted.pickle",
        "second": BLD / "weighted_data" / "PENDDAT_weighted.pickle",
    })
@pytask.mark.produces(SRC / "paper")
def task_creating_summary_stat_tex(depends_on, produces):
    names = ["PENDDAT", "HHENDDAT"]
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
