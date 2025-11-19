from dataclasses import dataclass
from functools import cached_property

import altair as alt
import geopandas as gpd
import polars as pl

from chart import Choropleth
from src.utils.clean import clean_column, clean_list_to_dataframe
from src.utils.fetch import fetch_acled_month, fetch_geojson
from geojson import get_region_list, build_geo_df
from .types import FeatureCollection


class PipelineRuntimeError(Exception):
    pass

@dataclass
class GeoAcled:
    country: str  = 'Mexico'
    year: int  = 2024
    month: int  = 1
    adm: str  = 'ADM1'
    def _fetch_acled(self) -> pl.DataFrame:
        try:
            acled_df = fetch_acled_month(self.country.title(),
                                            self.year, 
                                            self.month)
        except Exception as e:
            error_msg = 'Error fetching ACLED data'
            raise PipelineRuntimeError(f'{error_msg}: {e}') from e
        return acled_df

    def _fetch_geojson(self) -> tuple[FeatureCollection, str]:
        try:
            geojson, adm = fetch_geojson(self.country.lower(), self.adm)
        except Exception as e:
            error_msg = 'Error fetching geojson data'
            raise PipelineRuntimeError(f'{error_msg}: {e}') from e
        return geojson, adm

    def _join(self) -> pl.DataFrame:
        geojson, adm = self.geojson_adm_tuple
        cleaned_acled_df = clean_column(self.acled_df, adm)
        regions = get_region_list(geojson)
        cleaned_region_df = clean_list_to_dataframe(regions)
        try:
            joined_df = cleaned_acled_df.join(cleaned_region_df,
                                how='left',
                                on='cleaned_name')
            return joined_df
        except Exception as e:
            error_msg = 'Error joining acled data with geojson data'
            raise PipelineRuntimeError(f'{error_msg} : {e}') from e

    def _incident_count(self) -> pl.DataFrame:
        incidents_df = self.joined_df.group_by(
            'shapeName').len().rename({'len': 'incident_count'}
            )
        return incidents_df

    def _build_geo_df(self) -> gpd.GeoDataFrame:
        geojson, _ = self.geojson_adm_tuple
        return build_geo_df(geojson)

    def _build_chart(self) -> alt.Chart | alt.LayerChart | None:
        choropleth = None
        geojson, _ = self.geojson_adm_tuple
        if isinstance(
            self.incident_count_df, pl.DataFrame) and isinstance(
                geojson, dict):
            choropleth = Choropleth(lookup_df=self.incident_count_df,
                                    lookup_column='shapeName',
                                    geojson=geojson,
                                    geojson_id='shapeName' 
                                    )
        if choropleth is not None:
            return choropleth.as_chart()
        return None

    @cached_property
    def acled_df(self) -> pl.DataFrame:
        return self._fetch_acled()
    @cached_property
    def geojson_adm_tuple(self) -> tuple[FeatureCollection, str] | tuple[
        None, None]:
        return self._fetch_geojson()
    @cached_property
    def joined_df(self)-> pl.DataFrame | None:
        return self._join()
    @cached_property
    def incident_count_df(self)-> pl.DataFrame | None:
        return self._incident_count()
    def geo_df(self) -> gpd.GeoDataFrame:
        return self._build_geo_df()
    @cached_property
    def choropleth_chart(self) -> Choropleth | None:
        return self._build_chart()
