import geopandas as gpd

from .types import FeatureCollection


def get_region_list(geojson: FeatureCollection) -> set[str]:
    features = geojson['features']
    regions = []
    for feature in features:
        regions.append(feature['properties']['shapeName'])
    return set(regions)

def build_geo_df(geojson: FeatureCollection) -> gpd.GeoDataFrame:
    gdf = gpd.GeoDataFrame.from_features(geojson["features"])
    gdf.set_crs(4326, inplace=True)
    gdf['geometry'] = gdf.geometry.buffer(0)
    return gdf