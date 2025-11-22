import os

import dotenv
import polars as pl

from geoacled.acled.acled_query import AcledMonth
from geoacled.utils.date_range import date_range

_ = dotenv.load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB = os.getenv('DB')
DB_ADDRESS = os.getenv('DB_ADDRESS')

URI = f'postgresql://{DB_USER}:{DB_PASS}@{DB_ADDRESS}/{DB}'

def _build_filter_query(obj: AcledMonth
    ) -> str:
    start, end = date_range(obj.year, obj.month)
    return f"""
        SELECT *
        FROM acled_events
        WHERE country = '{obj.country}'
        AND event_date BETWEEN '{start}' and '{end}'
    """

def _build_filter_df(obj: AcledMonth) -> pl.DataFrame:
    try:
        return pl.read_database_uri(_build_filter_query(obj), URI)
    except Exception:
        return pl.DataFrame({'event_id_cnty': []})

def _filter_df(obj: AcledMonth) -> pl.DataFrame:
    filter_df = _build_filter_df(obj)
    return obj.df.filter(
        ~pl.col('event_id_cnty').is_in(filter_df['event_id_cnty']))

def acled_df_from_db(obj: AcledMonth) -> pl.DataFrame:
    return _build_filter_df(obj)

def acled_df_to_db(obj: AcledMonth) -> pl.DataFrame:
    df = _filter_df(obj)
    df.write_database('acled_events',
                                   connection=URI,
                                   if_table_exists='append')
    return df
