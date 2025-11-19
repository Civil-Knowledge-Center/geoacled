from dataclasses import dataclass
from functools import cached_property

import altair as alt
import geopandas as gpd
import polars as pl

from .chart import Choropleth
from .geojson import build_geo_df, get_region_list
from .types import FeatureCollection
from .utils.clean import clean_column, clean_list_to_dataframe
from .utils.fetch import fetch_acled_month, fetch_geojson


class PipelineRuntimeError(RuntimeError):
    def __init__(self, msg: str, e: Exception):
        super().__init__(f"{msg:} {e}")

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
            raise PipelineRuntimeError(error_msg, e) from e
        return acled_df

    def _fetch_geojson(self) -> tuple[FeatureCollection, str]:
        try:
            geojson, adm = fetch_geojson(self.country.lower(), self.adm)
        except Exception as e:
            error_msg = 'Error fetching geojson data'
            raise PipelineRuntimeError(error_msg, e) from e
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
        except Exception as e:
            error_msg = 'Error joining acled data with geojson data'
            raise PipelineRuntimeError(error_msg, e) from e
        return joined_df

    def _incident_count(self) -> pl.DataFrame:
        return self.joined_df.group_by(
            'shapeName').len().rename({'len': 'incident_count'}
            )

    def _build_geo_df(self) -> gpd.GeoDataFrame:
        geojson, _ = self.geojson_adm_tuple
        return build_geo_df(geojson)

    def _build_chart(self) -> alt.Chart | alt.LayerChart:
        choropleth = None
        geojson, _ = self.geojson_adm_tuple
        choropleth = Choropleth(lookup_df=self.incident_count_df,
                                lookup_column='shapeName',
                                geojson=geojson,
                                geojson_id='shapeName' 
                                )
        return choropleth.as_chart()


    @cached_property
    def acled_df(self) -> pl.DataFrame:
        return self._fetch_acled()
    @cached_property
    def geojson_adm_tuple(self) -> tuple[FeatureCollection, str]:
        return self._fetch_geojson()
    @cached_property
    def joined_df(self)-> pl.DataFrame:
        return self._join()
    @cached_property
    def incident_count_df(self)-> pl.DataFrame:
        return self._incident_count()
    def geo_df(self) -> gpd.GeoDataFrame:
        return self._build_geo_df()
    @cached_property
    def choropleth_chart(self) -> Choropleth:
        return self._build_chart()
