"""Module to pull a one month range for a single country from ACLED."""

from dataclasses import dataclass
from functools import cached_property

import httpx
import polars as pl

from acled.auth import authenticate
from utils import date_range

URL = 'https://acleddata.com/api/acled/read?_format=json'
TIMEOUT = httpx.Timeout(60.0, connect=10.0)


@dataclass(frozen=True)
class AcledMonth:
    """Class defines the country, year, and month to be queried."""

    country: str
    year: int
    month: int

    def _query_acled(self) -> httpx.Response:
        start, end = date_range(self.year, self.month)
        headers = {
            'Authorization': f'Bearer {authenticate()["access_token"]}',
            'Content-Type': 'application/json',
        }
        params = {
            'country': f'{self.country}',
            'event_date': f'{start}|{end}',
            'event_date_where': 'BETWEEN',
            'format': 'json',
        }
        with httpx.Client(timeout=TIMEOUT) as client:
            print(f'Query to ACLED: {params}')
            r = client.get(url=URL, params=params, headers=headers)
            r.raise_for_status()
            return r
    @cached_property
    def df(self) -> pl.DataFrame:
        """Returns a polars dataframe from an ACLED query."""
        return pl.DataFrame(self._query_acled().json()['data'])
