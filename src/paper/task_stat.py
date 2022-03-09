import glob
from pathlib import Path
from sys import platform

import pandas as pd
import pytask
import yaml

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
    name = []
    for i in range(len(files)):
        if any(x.isupper() for x in files[i].split(f"{b}")[-1].split("_")[0]):
            name.append(files[i].split(f"{b}")[-1].split("_")[0])
        else:
            name.append(
                files[i].split(f"{b}")[-1].split("_")[0]
                + "_"
                + files[i].split(f"{b}")[-1].split("_")[1]
            )
    return name


@pytask.mark.depends_on(BLD)
@pytask.mark.produces(SRC / "paper")
def task_creating_summary_stat_tex(depends_on, produces):
    names = get_names_dataset()
    for i in names:
        df = pd.read_pickle(str(Path(depends_on)) + f"/{i}_clean.pickle")
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
