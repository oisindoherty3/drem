"""Spatially map GNI consumption by Dublin postcode to a map, for either residential and
   non-residential buildings

- Standardise addresses using `pypostal`.
- Deduplicate standardised addresses to eliminate typos using `string_grouper`

Note: This module is not included in the prefect pipeline or tested as this would
require including libpostal in CI which would add a 2-3GB overhead...
"""

from pathlib import Path
from typing import Dict
from typing import List
import matplotlib.pyplot as plt

import pandas as pd
import geopandas as gpd

from icontract import require
from prefect import Flow
from prefect import task

from drem.filepaths import PROCESSED_DIR
from drem.filepaths import RAW_DIR
from drem.filepaths import EXTERNAL_DIR
from prefect.utilities.debug import raise_on_exception


@task
def _read_excel_file(filepath: Path) -> pd.DataFrame:

    return pd.read_excel(filepath)


@task
def _read_parquet_file(filepath: Path) -> pd.DataFrame:

    return gpd.read_parquet(filepath)


@task
def _reset_index(df: pd.DataFrame) -> pd.DataFrame:

    return df.reset_index()


# Maybe include a task to extract the neccesary rows


@task
@require(lambda df, mapping: set(mapping.keys()).issubset(set(df.columns)))
def _rename_columns(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:

    return df.rename(columns=mapping)


@task
def _strip_whitespace(df: pd.DataFrame, target: str, result: str) -> pd.DataFrame:

    df[result] = df[target].astype(str).str.replace(r"\s+", " ")

    return df


@task
@require(lambda df, column_names: set(column_names).issubset(set(df.columns)))
def _extract_columns(df: pd.DataFrame, column_names: List[str]) -> pd.DataFrame:

    return df.loc[:, column_names]


@task
@require(lambda gas, on: set(on).issubset(set(gas.columns)))
@require(lambda postcodes, on: set(on).issubset(set(postcodes.columns)))
def _merge_on_common_postcodes(
    gas: pd.DataFrame, postcodes: pd.DataFrame, on: List[str], validate, **kwargs,
) -> pd.DataFrame:

    return gas.merge(postcodes, on=on, validate=validate, **kwargs)


@task
def _plot_gas_by_postcode(
    df: gpd.GeoDataFrame, col, legend_title, filepath: Path
) -> gpd.GeoDataFrame:

    fig, ax = plt.subplots(1, 1, figsize=(10, 6))
    fig.suptitle(legend_title)
    gpd.GeoDataFrame(df).plot(column=col, cmap="plasma", legend=True)

    return plt.savefig(filepath)


with Flow("Plot Gas") as flow:

    gas_raw = _read_excel_file(RAW_DIR / "Gas_NonRes.xlsx")
    postcodes_raw = _read_parquet_file(PROCESSED_DIR / "dublin_postcodes.parquet")

    postcodes_reset = _reset_index(postcodes_raw)

    gas_renamed = _rename_columns(
        gas_raw,
        {
            "Table 4A Networked Gas Consumption by Dublin Postal District for Non-Residential Sector 2011-2019": "postcodes",
            "Unnamed: 9": "Consumption (GWh)",
        },
    )

    postcodes_stripped = _strip_whitespace(
        postcodes_reset, target="postcodes", result="postcodes",
    )
    gas_stripped = _strip_whitespace(
        gas_renamed, target="postcodes", result="postcodes",
    )

    gas_extracted = _extract_columns(gas_stripped, ["postcodes", "Consumption (GWh)"],)
    postcodes_extracted = _extract_columns(
        postcodes_stripped, ["postcodes", "geometry"],
    )

    gas_by_postcode = _merge_on_common_postcodes(
        gas_extracted,
        postcodes_extracted,
        on=["postcodes"],
        how="outer",
        validate="one_to_many",
    )

    _plot_gas_by_postcode(
        gas_by_postcode,
        "Consumption (GWh)",
        "Networked Gas Consumption by Dublin Postal District for Non-Residential Sector in 2019",
        (PROCESSED_DIR / "dublingasbypostcode.pdf"),
    )
