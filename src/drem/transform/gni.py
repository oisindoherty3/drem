"""Spatially map GNI consumption by Dublin postcode to a map, for both residential and
   non-residential buildings

- Standardise addresses using `pypostal`.
- Deduplicate standardised addresses to eliminate typos using `string_grouper`

Note: This module is not included in the prefect pipeline or tested as this would
require including libpostal in CI which would add a 2-3GB overhead...
"""

from pathlib import Path
from typing import Dict
from typing import List

import pandas as pd
import geopandas as gpd

from prefect import Flow
from prefect import task

from drem.filepaths import PROCESSED_DIR
from drem.filepaths import RAW_DIR


@task
def _read_excel_file(filepath: Path) -> pd.DataFrame:

    return pd.read_excel(filepath)


@task
def _read_shapefile(filepath: Path) -> pd.DataFrame:

    return gpd.read_file(filepath)


@task
@require(lambda df, mapping: set(mapping.keys()).issubset(set(df.columns)))
def _rename_columns(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:

    return df.rename(columns=mapping)


@task
@require(lambda df, column_names: set(column_names).issubset(set(df.columns)))
def _extract_columns(df: pd.DataFrame, column_names: List[str]) -> pd.DataFrame:

    return df.loc[:, column_names]


with Flow("Merge MPRN and GPRN") as flow:

    mprn_raw = _read_parquet_file(RAW_DIR / "mprn.parquet")
    gprn_raw = _read_parquet_file(RAW_DIR / "gprn.parquet")
    vo_raw = _read_parquet_file(PROCESSED_DIR / "vo_dublin.parquet")

    mprn_renamed = _rename_columns(
        mprn_aggregated,
        {"Attributable Total Final Consumption (kWh)": "electricity_demand_kwh_year"},
    )
    gprn_extracted = _extract_columns(
        gprn_deduped,
        [
            "deduped_address",
            "standardised_address",
            "Year",
            "summated_gas_demand_kwh_year",
        ],
    )
