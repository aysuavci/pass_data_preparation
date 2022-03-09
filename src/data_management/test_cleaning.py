import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_array_almost_equal
from numpy.testing import assert_equal

from src.config import BLD
from src.config import SRC

# from cleaning_functions import *
# from numpy.testing import assert_equal
# from pandas.testing import assert_frame_equal

# Inputs
# Define the variables


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
    df_p_c = pd.read_pickle(BLD / "PENDDAT_clean.pickle")
    return df_p_c


@pytest.fixture
def clean_data_h():
    df_h_c = pd.read_pickle(BLD / "HHENDDAT_clean.pickle")
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
        actual = df_p_c[["p_id", f"{y}"]].groupby(f"{y}").count().reset_index()
        assert_array_almost_equal(expected["pnr"], actual["p_id"])


def test_na(original_data_p, clean_data_p):
    df_p = original_data_p
    df_p_c = clean_data_p
    expected = (df_p < 0).any().any()
    actual = (df_p_c.fillna(0) >= 0).all().all()
    assert_equal(actual, expected)


"""
root_covs_np = np.zeros((nobs, nstates, nstates))
root_covs_np[:] = root_cov
root_covs_list = []
for _i in range(nobs):
    root_covs_list.append(
        pd.DataFrame(data=root_cov, columns=state_names, index=state_names)
    )

# create loadings np array  and dataframe
loadings_np = np.array([1.0, 0, 0, 0, 0])
loadings_pd = pd.Series(loadings_np, index=state_names)
# create measurements np array and dateframe
measurements_np = np.array([3.95733, 4.61214, 3.38793])
measurements_pd = pd.Series(measurements_np)
# construct the variance
meas_var = 0.8

# differentiate between pandas and numpy Inputs
kwargs_pandas = {
    "states": states_pd,
    "root_covs": root_covs_list,
    "measurements": measurements_pd,
    "loadings": loadings_pd,
    "meas_var": meas_var,
}


kwargs_numpy = {
    "states": states_np,
    "root_covs": root_covs_np,
    "measurements": measurements_np,
    "loadings": loadings_np,
    "meas_var": meas_var,
}

### TESTS



def test_fast_batch_update_state():
    expected_state, expected_root_covs = pandas_batch_update(**kwargs_pandas)
    # expected_root_covs=np.array(expected_root_covs)
    cal_state, cal_root_covs = fast_batch_update(**kwargs_numpy)
    assert_array_almost_equal(cal_state, expected_state)


def test_fast_batch_update_root_covs():
    expected_state, expected_root_covs = pandas_batch_update(**kwargs_pandas)
    cal_state, cal_root_covs = fast_batch_update(**kwargs_numpy)
    cal_root_covs = cal_root_covs.tolist()
    for i, root_cov in enumerate(cal_root_covs):
        expected_root_covs[i] = expected_root_covs[i].to_numpy()
        assert_array_almost_equal(expected_root_covs[i], cal_root_covs[i]) """
