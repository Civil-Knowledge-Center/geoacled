"""Module to pull a one month range for a single country from ACLED."""

from dataclasses import dataclass
from functools import cached_property

import httpx
import polars as pl

from geoacled.acled.auth import authenticate
from geoacled.utils.date_range import date_range

URL = 'https://acleddata.com/api/acled/read?_format=json'
TIMEOUT = httpx.Timeout(60.0, connect=10.0)
ACLED_PAGE_LIMIT = 5000

def _query_acled(country: str, start: str, end: str, page: int | None = None ) -> httpx.Response:
        headers = {
            'Authorization': f'Bearer {authenticate()["access_token"]}',
            'Content-Type': 'application/json',
        }
        params = {
            'country': f'{country}',
            'event_date': f'{start}|{end}',
            'event_date_where': 'BETWEEN',
            'format': 'json',
        }
        if page:
             params['page'] = str(page)
        with httpx.Client(timeout=TIMEOUT) as client:
            print(f'Query to ACLED: {params}')
            r = client.get(url=URL, params=params, headers=headers)
            r.raise_for_status()
            return r.json()

@dataclass(frozen=True)
class AcledMonth:
    """Class defines the country, year, and month to be queried."""

    country: str
    year: int
    month: int

    @cached_property
    def df(self) -> pl.DataFrame:
        """Returns a polars dataframe for one month ACLED query."""
        start, end = date_range(self.year, self.month) 
        return pl.DataFrame(_query_acled(self.country, start, end)['data'])

@dataclass(frozen=True)
class AcledYear:

    country: str
    year_start: str
    year_end: str

    @cached_property
    def df(self) -> pl.DataFrame:
        """Returns a polars dataframe for one year of ACLED data."""
        concat_df = pl.DataFrame()
        page = 1
        height = 5000
        
        while height == 5000:
            fetch_df = pl.DataFrame(_query_acled(self.country,
                                                self.year_start,
                                                self.year_end,
                                                page).json()['data'])
            concat_df = pl.concat([concat_df, fetch_df])
            page += 1
            height = fetch_df.height
        return concat_df
