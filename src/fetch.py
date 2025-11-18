import httpx
import pycountry

from acled import AcledMonth
from acled.acled_db import acled_df_from_db, acled_df_to_db


def fetch_acled_month(country: str, year: int, month: int):
    obj = AcledMonth(country, year, month)
    existing = acled_df_from_db(obj)
    if not existing.is_empty():
        return existing
    return acled_df_to_db(obj)

def fetch_geojson(country_name) -> tuple[dict[str, str], str]:
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