from dataclasses import dataclass, field
from typing import Literal, Mapping
import altair as alt
import polars as pl

@dataclass
class Choropleth:
    lookup_df: pl.DataFrame
    lookup_column: str
    geojson: dict = field(default_factory=dict)
    geojson_id: str = 'SIGLA'
    feature_key: str = 'features'
    theme: str = 'dark'
    points: alt.Chart | None = None
    point_labels: alt.Chart | None = None
    basemap: alt.Chart | None = None
    points_df: pl.DataFrame | None = None
    points_marker_size: int = 50
    points_marker_color: Literal['black', 'crimson'] = 'crimson'
    points_label_column: str | None = None
    points_label_align: Literal['center','left','right'] = 'right'
    points_label_x_offset: int = -6
    points_label_y_offset: int = -2
    points_label_font_size: int = 12
    points_label_font_weight: Literal['normal', 'bold','lighter'] = 'bold'
    points_label_text_color: str = 'black'
    points_label_color: str = 'crimson'
    points_lat_column: str = 'lat'
    points_lng_column: str = 'lng'
    basemap_stroke_color: Literal['white', 'black'] = 'white'
    basemap_stroke_width: float = 0.5
    basemap_color_column: str = 'incident_count'
    basemap_color_scheme: Literal['reds', 'blueorange']= 'blueorange'
    basemap_tooltips: Mapping[str, str] | None = field(default_factory=lambda: 
                                                {'properties.Estado': 'State',
                                           'incident_count': 'Incidents'})
    width: int = 600
    height: int = 600
    title: str = 'Total Incidents of Political Unrest 2022-2024'
    projection: Literal['mercator'] = 'mercator'
    def _build_points(self) -> None | alt.Chart:
        if self.points_df is None:
            return None
        return (alt.Chart(self.points_df)
            .mark_circle(
                size=self.points_marker_size,
                color=self.points_marker_color)
            .encode(
                longitude=f"{self.points_lng_column}:Q",
                latitude=f"{self.points_lat_column}:Q",
                tooltip=[f"{self.points_label_column}:N"]
                )
        )

    def _build_point_labels(self) -> None | alt.Chart:
        if self.points_df is None or self.points_label_column is None:
            return None
        return (alt.Chart(self.points_df)
            .mark_text(
                align=self.points_label_align,
                dx=self.points_label_x_offset,  
                dy=self.points_label_y_offset, 
                fontSize=self.points_label_font_size,
                fontWeight=self.points_label_font_weight,
                color=self.points_label_text_color,
                stroke='grey',
                strokeWidth=.5
            )
            .encode(
                longitude=f"{self.points_lng_column}:Q",
                latitude=f"{self.points_lat_column}:Q", 
                text=f"{self.points_label_column}:N")
            )
    def _build_tooltips(self) -> None:
        self.processed_tooltips = [
            alt.Tooltip(field=field, title=title)
            for field, title in self.basemap_tooltips.items()
        ]
    def _build_base_map(self) -> alt.Chart:
        return(
            alt.Chart(
                alt.Data(
                    values=self.geojson[self.feature_key]
                ))
                .mark_geoshape(
                    stroke=self.basemap_stroke_color,
                    strokeWidth=self.basemap_stroke_width
                )
                .encode(
                    color=alt.Color(f"{self.basemap_color_column}:Q",
                    scale=alt.Scale(scheme=self.basemap_color_scheme)),
                    tooltip=self.processed_tooltips
                )
                .transform_lookup(
                    lookup=f"properties.{self.geojson_id}",
                    from_=alt.LookupData(self.lookup_df,
                                         self.geojson_id,
                                         [self.lookup_column])
                )
                .properties(width=self.width,
                            height=self.height,
                            title=self.title
                )
                .project(type=self.projection)
        )
    def as_chart(self) -> alt.Chart | alt.LayerChart| None:
        if self.basemap_tooltips is not None:
            self._build_tooltips()
        self.basemap = self._build_base_map()
        if self.points_df is None or self.points_label_column is None and self.basemap:
            return self.basemap
        self.points = self._build_points()
        self.point_labels = self._build_point_labels()
        if self.basemap and self.points and self.point_labels: 
            return self.basemap + self.points + self.point_labels
        return None