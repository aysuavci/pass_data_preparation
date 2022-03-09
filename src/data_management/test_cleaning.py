import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_array_almost_equal
from numpy.testing import assert_equal

from src.config import BLD
from src.config import SRC

# from cleaning_functions import *
# from pandas.testing import assert_frame_equal


@pytest.fixture
def original_data_p():
    df_p = pd.read_stata(
        SRC / "original_data" / "PENDDAT_cf_W11.dta", convert_categoricals=False
    )
    return df_p


@pytest.fixture
def original_data_h():
    df_h = pd.read_stata(
        SRC / "original_data" / "HHENDDAT_cf_W11.dta", convert_categoricals=False
    )
    return df_h


@pytest.fixture
def clean_data_p():
    df_p_c = pd.read_pickle(BLD / "weighted_data/PENDDAT_weighted.pickle")
    return df_p_c


@pytest.fixture
def clean_data_h():
    df_h_c = pd.read_pickle(BLD / "weighted_data/HHENDDAT_weighted.pickle")
    return df_h_c


def test_reversing(original_data_p, clean_data_p):
    df_p = original_data_p
    df_p[
        df_p < 0
    ] = (
        np.nan
    )  # dropped na because otherwise there is gonna be negative values appeared
    df_p_c = clean_data_p
    dict1 = {
        "PEO1400a": "b5_ext_a",
        "PEO1400b": "b5_agree_a",
        "PEO1400h": "b5_consc_b",
        "PEO1400i": "b5_neu_b",
        "PEO1400k": "b5_ext_c",
        "PEO1400l": "b5_agree_c",
        "PEO1400u": "b5_open_e",
        "PQB0600e": "eri_reward_b",
        "PQB0600f": "eri_reward_c",
        "PQB0600g": "eri_reward_d",
    }  # dictionary for variables which should be reversed
    for x, y in dict1.items():
        expected = df_p[["pnr", f"{x}"]].groupby(f"{x}").count()
        expected["pnr"] = expected["pnr"].values[::-1]  # reversing the order
        expected = expected.reset_index()
        actual = df_p_c.reset_index()
        actual = actual[["p_id", f"{y}"]].groupby(f"{y}").count()
        assert_array_almost_equal(expected["pnr"], actual["p_id"])


def test_na(original_data_p, clean_data_p):
    df_p = original_data_p
    df_p_c = clean_data_p
    expected = (df_p < 0).any().any()
    actual = (df_p_c.fillna(0) >= 0).all().all()
    assert_equal(actual, expected)
