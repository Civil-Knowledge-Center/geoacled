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

def _query_acled(country: str | None = None,
                 iso: int | None = None,
                 start: str | None = None,
                 end: str | None = None,
                 page: int | None = None ) -> httpx.Response:
        headers = {
            'Authorization': f'Bearer {authenticate()["access_token"]}',
            'Content-Type': 'application/json',
        }
        params = {

            'event_date': f'{start}|{end}',
            'event_date_where': 'BETWEEN',
            'format': 'json',
        }
        if country:
             params['country'] = country
        if iso:
             params['iso'] = str(iso)
        if not country and not iso:
             raise ValueError('Must supply country or numeric iso code')
        if page:
             params['page'] = str(page)
        with httpx.Client(timeout=TIMEOUT) as client:
            print(f'Query to ACLED: {params}')
            r = client.get(url=URL, params=params, headers=headers)
            r.raise_for_status()
            return r

@dataclass(frozen=True)
class AcledMonth:
    """Class defines the country, year, and month to be queried."""

    country: str | None = None
    iso: int | None = None
    year: int  = 2021
    month: int = 1

    @cached_property
    def df(self) -> pl.DataFrame:
        """Returns a polars dataframe for one month ACLED query."""
        start, end = date_range(self.year, self.month) 
        return pl.DataFrame(_query_acled(country=self.country, 
                                         iso=self.iso,
                                         start=start, 
                                         end=end).json()['data'])

@dataclass(frozen=True)
class AcledYear:

    country: str | None = None
    iso: int | None = None 
    year_start: str | None = '2021-01-01'
    year_end: str | None = '2021-12-31'

    @cached_property
    def df(self) -> pl.DataFrame:
        """Returns a polars dataframe for one year of ACLED data."""
        concat_df = pl.DataFrame()
        page = 1
        height = 5000
        
        while height == 5000:
            fetch_df = pl.DataFrame(_query_acled(country=self.country,
                                                iso=self.iso,
                                                start=self.year_start,
                                                end=self.year_end,
                                                page=page).json()['data'])
            concat_df = pl.concat([concat_df, fetch_df])
            page += 1
            height = fetch_df.height
        return concat_df
