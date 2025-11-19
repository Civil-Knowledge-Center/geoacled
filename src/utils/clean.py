import unicodedata

import polars as pl


def strip_accents(text: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFKD', text)
        if not unicodedata.combining(c)
    )

def clean_column(df: pl.DataFrame, adm: str | None,
                 alias: str|None = 'cleaned_name') -> pl.DataFrame:
    match adm:
        case 'ADM1':
            col = 'admin1'
        case 'ADM2':
            col = 'admin2'
        case _:
            col = 'admin2'
    alias = alias or col
    return df.with_columns(
        pl.col(col).map_elements(
        strip_accents,
        return_dtype=pl.Utf8)
        .str.strip_chars()
        .str.to_lowercase()
        .alias(alias))

def clean_list_to_dataframe(lst: list[str],
                            original: str | None = 'shapeName',
                            cleaned: str | None = 'cleaned_name'):
    original_list = []
    cleaned_list = []
    for val in lst:
        original_list.append(val)
        cleaned_list.append(strip_accents(val).lower().strip())
    return pl.DataFrame({original:original_list, cleaned: cleaned_list})