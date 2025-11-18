import polars as pl

from clean import clean_column, clean_list_to_dataframe
from fetch import fetch_acled_month, fetch_geojson
from geojson import get_region_list

#Need to normalize capitalization

class PipelineRuntimeError(Exception):
    pass

def run_pipeline(country: str | None = 'Mexico',
                 year: int | None = 2024,
                 month: int | None = 1) -> pl.DataFrame | None:
    acled_df = None
    geojson = None
    adm = None
    joined_df = None
    if country and year and month:
        try:
            acled_df = fetch_acled_month(country.title(), year, month)
        except Exception as e:
            error_msg = 'Error fetching ACLED data'
            raise PipelineRuntimeError(error_msg) from e
        try:
            geojson, adm = fetch_geojson(country.lower())
        except Exception as e:
            error_msg = 'Error fetching geojson data'
            raise PipelineRuntimeError(error_msg) from e
    if (isinstance(acled_df, pl.DataFrame)
        and isinstance(geojson, dict)):
        cleaned_acled_df = clean_column(acled_df, adm)
        regions = get_region_list(geojson)
        if regions:
            cleaned_region_df = clean_list_to_dataframe(regions)
            try:
                joined_df = cleaned_acled_df.join(cleaned_region_df,
                                      how='left',
                                      on='state_name')
            except Exception as e:
                error_msg = 'Error joining acled data with geojson data'
                raise PipelineRuntimeError(error_msg) from e
    if isinstance(joined_df, pl.DataFrame):
        return joined_df
    return None