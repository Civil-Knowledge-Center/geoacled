import httpx
import pycountry


def get_geojson(country_name) -> tuple[dict[str, str], str]:
    try:
        country = pycountry.countries.get(name=country_name)
        if not country:
            raise ValueError(f"Country '{country_name}' not found in pycountry.")
        r = httpx.get(
            f"https://www.geoboundaries.org/api/current/gbOpen/{country.alpha_3}/ALL/"
        )
        r.raise_for_status()
        geourl, adm = r.json()[-1]["gjDownloadURL"], r.json()[-1]["boundaryType"]
        geo_r = httpx.get(geourl, follow_redirects=True)
        r.raise_for_status()
        if not geo_r.text.strip().startswith("{"):
            raise ValueError("Invalid GeoJSON returned")
        return geo_r.json(), adm
    except Exception as e:
        raise RuntimeError(f"Failed to fetch GeoJSON for {country_name}") from e
