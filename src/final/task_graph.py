import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytask

from src.config import BLD


@pytask.mark.depends_on(
    {
        "first": BLD / "weighted_data" / "PENDDAT_weighted.pickle",
        "second": BLD / "weighted_data" / "PENDDAT_weighted.pickle",
    }
)
@pytask.mark.produces(
    {
        "first": BLD / "figures" / "number_obs.png",
        "second": BLD / "figures" / "age_dist.png",
        "third": BLD / "figures" / "gender_dist.png",
        "fourth": BLD / "figures" / "gender_role.png",
    }
)
def task_plotting_graphs(depends_on, produces):
    df_p_w = pd.read_pickle(depends_on["first"])
    df_h_w = pd.read_pickle(depends_on["second"])
    # number of observation per wave
    x_p = df_p_w.reset_index().groupby("wave").count().reset_index()["wave"]
    y_p = df_p_w.reset_index().groupby("wave").count().reset_index()["p_id"]
    x_h = df_h_w.reset_index().groupby("wave").count().reset_index()["wave"]
    y_h = df_h_w.reset_index().groupby("wave").count().reset_index()["hh_id"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 7))
    ax1.stem(x_p, y_p)
    ax2.stem(x_h, y_h)
    ax1.set_ylabel("Number of Observations", fontsize="x-large")
    fig.suptitle("Number of Observations per Wave", fontsize="xx-large")
    ax1.set_title("Personal Dataset")
    ax2.set_title("Household Dataset")
    fig.text(0.465, 0.04, "Wave Number", fontsize="x-large")
    plt.savefig(produces["first"])

    # number of observation per age
    x_p = df_p_w.reset_index()
    x_p = x_p[x_p["wave"] == 11].groupby("age").count()["p_id"].reset_index()["age"]
    y_p = df_p_w.reset_index()
    y_p = y_p[y_p["wave"] == 11].groupby("age").count()["p_id"].reset_index()["p_id"]

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.bar(x_p, y_p)
    ax.set_ylabel("Number of Observation", fontsize="x-large")
    ax.set_xlabel("Age", fontsize="x-large")
    ax.set_title("Age Distribution in Wave 11", fontsize="xx-large")
    plt.savefig(produces["second"])

    # Gender Distribution
    x_p = df_p_w.reset_index()
    x_p = x_p[x_p["wave"] == 11].groupby("sex").count()["p_id"].reset_index()["sex"]
    y_p = df_p_w.reset_index()
    y_p = y_p[y_p["wave"] == 11].groupby("sex").count()["p_id"].reset_index()["p_id"]

    fig, ax = plt.subplots(figsize=(8, 7))
    ax.bar(x_p, y_p, width=0.6)
    plt.xticks([1, 2], ["Male", "Female"])
    ax.set_xlabel("Gender", fontsize="x-large")
    ax.set_ylabel("Number of Observation", fontsize="x-large")
    plt.title("Gender Distribution in Wave 11", fontsize="xx-large")
    plt.savefig(produces["third"])

    # genrole_modern, genrole_traditional
    x_p = df_p_w.reset_index()
    x_p = (
        x_p[x_p["wave"] == 11]
        .groupby("sex")
        .mean()[["genrole_modern", "genrole_traditional"]]
        .reset_index()
    )
    x_1 = x_p["sex"]
    y_1 = x_p["genrole_traditional"]
    y_2 = x_p["genrole_modern"]
    x_p = df_p_w.reset_index()
    x_p = (
        x_p[x_p["wave"] == 11]
        .groupby("age")
        .mean()[["genrole_modern", "genrole_traditional"]]
        .reset_index()
    )
    x_2 = x_p["age"]
    y_1_sex = x_p["genrole_traditional"]
    y_2_sex = x_p["genrole_modern"]
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    ax1.plot(x_1, y_1, "o", ls="-", label="Traditional")
    ax1.plot(x_1, y_2, "o", ls="-", label="Modern")
    ax1.set_xbound(0.60, 2.40)
    ax1.legend(loc="center right")
    ax1.set_xlabel("Gender", fontsize="x-large")
    ax1.set_ylabel("Gender Attitude Scales", fontsize="x-large")
    ax1.set_title("By Gender", fontsize="x-large")
    ax1.set_xticks([1, 2], ["Male", "Female"])

    ax2.plot(x_2, y_1_sex, label="Traditional")
    ax2.plot(x_2, y_2_sex, label="Modern")
    ax2.legend()
    z_1 = np.polyfit(x_2, y_1_sex, 1)
    p_1 = np.poly1d(z_1)
    ax2.plot(x_2, p_1(x_2), "r--")
    z_2 = np.polyfit(x_2, y_2_sex, 1)
    p_2 = np.poly1d(z_2)
    ax2.plot(x_2, p_2(x_2), "r--")
    ax2.set_xlabel("Age", fontsize="x-large")
    ax2.set_title("By Age", fontsize="x-large")
    fig.suptitle("Gender Role Attitudes", fontsize="xx-large")
    plt.savefig(produces["fourth"])
