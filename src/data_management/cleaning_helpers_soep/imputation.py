"""
This file contains some functions for the imputation of missing values
"""
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf


def impute_var_one_year(missing_col, feature_cols, df, df_pred, numerical, rounding):
    """Impute a column that contains missings based on a list of
    feature columns for one year

    Args:
        missing_col (string): column for which values should be imputed
        feature_cols (list): columns that should be used for the prediction
            of missing values
        df (DataFrame): DataFrame used for estimating the relation of
        feature_cols and missing_col
        df_pred (DataFrame): DataFrame for which prediction/imputation is made
        numerical (bool): if feature_col is a numerical variable
        rounding (int): number of digits to round the result to

    Returns:
        Series: column missing_col in df_pred including both observed and
            imputed values

    """

    df = df.copy()
    if numerical:
        # Make sure no observations are missing for feature_cols variables
        if df[feature_cols].isna().sum().sum() > 0:
            columns_with_missings = (
                df[feature_cols].loc[:, df[feature_cols].isna().any()].columns
            )
            raise ValueError(
                "The following feature columns contain "
                f"missing observations: {columns_with_missings}"
            )

        temp = df.dropna(subset=[missing_col])
        mod = smf.ols(formula=f"{missing_col} ~ " + " + ".join(feature_cols), data=temp)
        res = mod.fit()
        # print(res.summary())

        # Make prediction
        imputed = res.predict(df_pred.loc[df_pred[missing_col].isna()][feature_cols])

        # Cut at minimum and maximum observed value
        imputed = imputed.clip(df[missing_col].min(), df[missing_col].max())

        # Round result if specified
        if rounding is not None:
            imputed = np.round(imputed, rounding)

        # Output
        # print("imputed", imputed.count(), imputed.mean(),
        #  imputed.min(), imputed.max())
        # print(
        #     "existing",
        #     df[missing_col].count(),
        #     df[missing_col].mean(),
        #     df[missing_col].min(),
        #     df[missing_col].max(),
        # )

        df_pred.loc[df_pred[missing_col].isna(), missing_col] = imputed

    else:
        raise NotImplementedError("only numerical imputation implemented currently")

    return df_pred[missing_col]


def impute_var(
    missing_col,
    feature_cols,
    df,
    numerical=True,
    rounding=None,
    exclude_years=None,
):
    """Impute a column that contains missings based on a list of
    feature columns
    """
    exclude_years = {} if exclude_years is None else exclude_years
    df = df.copy()

    # Make sure that jahr is an index column
    if "jahr" in df.columns:
        df = df.set_index(["p_id", "jahr"])

    if "jahr" in df.index.names and len(df.index.unique(level="jahr")) > 1:
        res = []
        for jahr in df.index.unique(level="jahr"):
            df_y = df.query(f"jahr == {jahr}").copy()
            if jahr not in exclude_years:
                print(f"jahr: {jahr}")
                df_y[missing_col] = impute_var_one_year(
                    missing_col,
                    feature_cols,
                    df_y,
                    df_pred=df_y,
                    numerical=numerical,
                    rounding=rounding,
                )
            elif jahr in exclude_years and exclude_years[jahr]:
                print(f"jahr: {jahr} making use of year {exclude_years[jahr]}")
                df_other = df.query(f"jahr == {exclude_years[jahr]}").copy()
                df_y[missing_col] = impute_var_one_year(
                    missing_col,
                    feature_cols,
                    df_other,
                    df_pred=df_y,
                    numerical=numerical,
                    rounding=rounding,
                )
            res.append(df_y[missing_col])

        res = pd.concat(res)
        res.index.name = "jahr"
        res = res.sort_index()
        return res

    else:
        return impute_var_one_year(
            missing_col,
            feature_cols,
            df,
            df_pred=df,
            numerical=numerical,
            rounding=rounding,
        )
