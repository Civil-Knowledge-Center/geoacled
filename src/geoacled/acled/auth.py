"""Module to retrieve oauth token from ACLED."""

import datetime
import json
import os

import dotenv
import httpx

dotenv.load_dotenv()

ACLED_EMAIL = os.getenv("ACLED_EMAIL")
ACLED_PASS = os.getenv("ACLED_PASS")
CACHE_FILE = os.getenv("CACHE_FILE")
ACLED_AUTH_URL = "https://acleddata.com/oauth/token"
ACLED_REFRESH_TTL = 14
HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}
TZ = datetime.UTC

AUTH_DICT = {
    "username": ACLED_EMAIL,
    "password": ACLED_PASS,
    "grant_type": "password",
    "client_id": "acled",
}

class AuthenticationError(RuntimeError):
    """Catch exceptions raised during authentication."""

    def __init__(self, exception: Exception) -> None:
        """Print exceptions raised during authentication."""
        super().__init__(
            f"Failed to fetch authentication token: {exception}")
        self.exception = exception

def authenticate() -> dict[str, str | int]:
    """Use 'ACLED_EMAIL' and 'ACLED_PASS' defined in .env.

    Returns oauth token as JSON.
    Writes to 'CACHE_FILE' defined in .env
    """
    try:
        now_ts_int = int(datetime.datetime.now(TZ).timestamp())
        token = _read_cache()
        if not token:
            return _get_token(None)
        if now_ts_int > int(token.get("refresh_expiration_time", 0)):
            return _get_token(None)
        if now_ts_int > int(token.get("expiration_time", 0)):
            return _refresh_token(str(token["refresh_token"]))
        return token  # noqa: TRY300
    except Exception as e:
        raise AuthenticationError(e) from e


def _get_token(data: dict[str, str | None] | None) -> dict[str, str | int]:
    if data is None:
        data = AUTH_DICT
    r = httpx.post(ACLED_AUTH_URL, headers=HEADERS, data=data)
    r.raise_for_status()
    r_json = r.json()
    r_json = _set_expiration_times(r_json)
    _write_cache(r_json)
    return r_json


def _refresh_token(refresh_token: str) -> dict[str, str | int]:
    refresh_dict = {
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
        "client_id": AUTH_DICT["client_id"],
    }
    return _get_token(data=refresh_dict)


def _read_cache() -> dict[str, str | int] | None:
    if not CACHE_FILE or not os.path.exists(CACHE_FILE):
        return None
    with open(CACHE_FILE, encoding='utf-8') as infile:
        return json.load(infile)


def _write_cache(r_json: dict[str, str | int]) -> None:
    if CACHE_FILE:
        with open(CACHE_FILE, "w", encoding='utf-8') as outfile:
            json.dump(r_json, outfile)


def _set_expiration_times(
        r_json: dict[str, str | int]) -> dict[str, str | int]:
    now = datetime.datetime.now(TZ)
    expiration_time = now + datetime.timedelta(
        seconds=int(r_json["expires_in"]))
    refresh_expiration_time = now + datetime.timedelta(days=ACLED_REFRESH_TTL)
    r_json["expiration_time"] = int(expiration_time.timestamp())
    r_json["refresh_expiration_time"] = int(
        refresh_expiration_time.timestamp())
    return r_json
