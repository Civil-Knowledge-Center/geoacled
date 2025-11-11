import os

import dotenv
import polars as pl
import sqlalchemy as sa

_ = dotenv.load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB = 'acled'
DB_ADDRESS = 'localhost:5432'

URI = f'postgresql://{DB_USER}:{DB_PASS}@{DB_ADDRESS}/{DB}'

def build_filter_query(country: str, start: str, stop: str
    ) -> str:
    return f"""
        FROM acled
        WHERE country = '{country}'
        AND event_date BETWEEN '{start}' and '{stop}'
    """

def write_df(df: pl.DataFrame,
             table: str,
             unique_column: str,
             query: str) -> None:
    engine = sa.create_engine(URI)
    inspector = sa.inspect(engine)
    with engine.begin() as conn:
        if not inspector.has_table(table):
            _ = df.write_database(
                    table_name=table,
                    connection=URI,
                    if_table_exists='replace'
                )
            _ = conn.execute(sa.text(
                f"""
                ALTER TABLE {table}
                ADD CONSTRAINT unique_event UNIQUE (f{unique_column})
                """))
        else:
            filter_df = pl.read_database_uri(query, URI)