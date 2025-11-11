from dataclasses import dataclass, field
import json
import polars as pl
import altair as alt
from utils import clean_column

@dataclass
class AcledData: 
    feature_key: str = 'features'
    property_keys: list[str] = field(default_factory=lambda: ['SIGLA', 'Estado', 'Total'])
    acled_csv: str = 'data/acled_data_br_2022-2024.csv'
    join_columns: tuple[str,str] = ('admin1', 'Estado')
    geojson_id: str = 'SIGLA'
    geojson_file: str | None = 'data/br_states.json'
    geojson: dict = field(default_factory=dict)

    def __post_init__(self):
        self._build_acled_df()
        if self.geojson_file is not None:
            self._load_geojson()
            self._build_geo_df() 

    def _load_geojson(self) -> None:
        if self.geojson_file is None:
            return
        with open(self.geojson_file, 'r', encoding='utf-8') as f:
            self.geojson = json.load(f)

    def _build_geo_df(self) -> None:
        if self.geojson_file is None:
            return
        features = self.geojson[self.feature_key]
        records = []
        for feature in features:
            props = feature.get('properties', {})
            record = {k: props.get(k) for k in self.property_keys}
            record['geometry_type'] = feature['geometry']['type']
            records.append(record)
        self.geo_df = pl.DataFrame(records)

    def _build_acled_df(self) -> None:
        self.acled_df = pl.read_csv(self.acled_csv)

    def join_on_geojson_id(self) -> pl.DataFrame:
        left, right = self.join_columns
        acled_clean = clean_column(self.acled_df, left, f"{left}_clean")
        geo_clean = clean_column(self.geo_df, right, f"{right}_clean")
        return acled_clean.join(
            geo_clean.select([self.geojson_id, f"{right}_clean"]),
            left_on=f"{left}_clean",
            right_on=f"{right}_clean",
            how='left'
        )        