def get_region_list(geojson: dict[str,str]) -> list[str] | None:
    features = geojson['features']
    regions = []
    for feature in features:
        regions.append(feature['properties']['shapeName'])
    if regions:
        return regions
    return None