"""
Query the Copernicus Data Space Ecosystem STAC API through a legacy-compatible
OpenSearch-like interface.
"""

import json
import re
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from random import random
from time import sleep
from typing import Any, Dict, Optional, Union

from requests.exceptions import ChunkedEncodingError, RequestException
from urllib3.exceptions import ProtocolError

from cdsetool.credentials import Credentials
from cdsetool.logger import NoopLogger
from cdsetool.stac.compat import stac_item_to_cdse_feature
from cdsetool.stac.translate import (
    STAC_COLLECTIONS_URL,
    STAC_SEARCH_URL,
    build_stac_search_payload,
    resolve_stac_collections,
)


class _FeatureIterator:
    def __init__(self, feature_query) -> None:
        self.index = 0
        self.feature_query = feature_query

    def __len__(self) -> int:
        return len(self.feature_query)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            item = self.feature_query[self.index]
            self.index += 1
            return item
        except IndexError as exc:
            raise StopIteration from exc


class FeatureQuery:
    """
    An iterator over features matching search terms.

    Results are fetched in pages from STAC and converted to the legacy feature
    structure expected by existing callers.
    """

    total_results: int = -1

    def __init__(
        self,
        collection: str,
        search_terms: Dict[str, Any],
        proxies: Union[Dict[str, str], None] = None,
        options: Union[Dict[str, Any], None] = None,
    ) -> None:
        self.features = []
        self.total_results = -1
        self.collection = collection
        self.proxies = proxies
        self.log = (options or {}).get("logger") or NoopLogger()

        validate_search_terms = (options or {}).get(
            "validate_search_terms", True)
        if validate_search_terms:
            _validate_search_terms(collection, search_terms, proxies)

        payload = build_stac_search_payload(collection, search_terms)
        self.next_request = {
            "method": "POST",
            "url": STAC_SEARCH_URL,
            "json": payload,
        }

        # Eagerly fetch first page to preserve callers that read `.features` directly.
        self.__fetch_features()

    def __iter__(self):
        return _FeatureIterator(self)

    def __len__(self) -> int:
        if self.total_results >= 0:
            return self.total_results

        while self.next_request is not None:
            self.__fetch_features()

        self.total_results = len(self.features)
        return self.total_results

    def __getitem__(self, index: int):
        while index >= len(self.features) and self.next_request is not None:
            self.__fetch_features()

        return self.features[index]

    def __fetch_features(self) -> None:
        if self.next_request is None:
            return

        session = Credentials.make_session(
            None, False, Credentials.RETRIES, self.proxies
        )

        attempts = 0
        while attempts < 5:
            attempts += 1
            try:
                request = self.next_request
                method = (request.get("method") or "POST").upper()

                if method == "GET":
                    response_ctx = session.get(request["url"])
                else:
                    response_ctx = session.post(
                        request["url"], json=request.get("json") or {}
                    )

                with response_ctx as response:
                    if response.status_code != 200:
                        self.log.warning(
                            f"Status code {response.status_code}, retrying.."
                        )
                        if response.status_code in (400, 401, 403, 404):
                            response.raise_for_status()

                        retry_after_seconds = _retry_after_seconds(
                            response.headers.get("Retry-After")
                        )
                        if retry_after_seconds is not None:
                            sleep(retry_after_seconds)
                        else:
                            sleep(5 * (1 + (random() / 4)))
                        continue

                    try:
                        res = response.json()
                    except ValueError as error:
                        self.log.warning(error)
                        sleep(1 * (1 + (random() / 4)))
                        continue

                mapped_features = [
                    stac_item_to_cdse_feature(item, self.collection)
                    for item in (res.get("features") or [])
                ]
                self.features.extend(mapped_features)

                total_results = res.get("numberMatched")
                if total_results is None:
                    total_results = (res.get("context") or {}).get("matched")
                if total_results is not None:
                    self.total_results = total_results

                self.__set_next_request(res)
                if self.next_request is None and self.total_results < 0:
                    self.total_results = len(self.features)
                return
            except (
                ChunkedEncodingError,
                ConnectionResetError,
                ProtocolError,
                RequestException,
            ) as error:
                self.log.warning(error)
                continue

        raise RuntimeError(
            f"Failed to query STAC after {attempts} attempts for {self.collection}"
        )

    def __set_next_request(self, res: Dict[str, Any]) -> None:
        links = res.get("links") or []
        next_link = next(
            (link for link in links if link.get("rel") == "next"), None)
        if not next_link:
            self.next_request = None
            return

        method = (next_link.get("method") or "GET").upper()
        request: Dict[str, Any] = {
            "method": method,
            "url": next_link.get("href") or STAC_SEARCH_URL,
        }
        if method == "POST":
            request["json"] = next_link.get("body") or {}
        self.next_request = request


def query_features(
    collection: str,
    search_terms: Dict[str, Any],
    proxies: Union[Dict[str, str], None] = None,
    options: Union[Dict[str, Any], None] = None,
) -> FeatureQuery:
    """
    Return an iterator over features matching the search terms.
    """
    return FeatureQuery(
        collection, {"maxRecords": 2000, **search_terms}, proxies, options
    )


def shape_to_wkt(shape: str) -> str:
    """
    Convert a shapefile to a WKT string.
    """
    import geopandas as gpd  # Lazy import to avoid hard dependency for query_features().

    coordinates = list(gpd.read_file(shape).geometry[0].exterior.coords)
    return (
        "POLYGON(("
        + ", ".join(" ".join(map(str, coord)) for coord in coordinates)
        + "))"
    )


def geojson_to_wkt(geojson_in: Union[str, Dict]) -> str:
    """
    Convert a geojson geometry to a WKT string.
    """
    geojson = json.loads(geojson_in) if isinstance(
        geojson_in, str) else geojson_in

    if geojson.get("type") == "Feature":
        geojson = geojson["geometry"]
    elif geojson.get("type") == "FeatureCollection" and len(geojson["features"]) == 1:
        geojson = geojson["features"][0]["geometry"]

    coordinates = str(
        tuple(item for sublist in geojson["coordinates"][0]
              for item in sublist)
    )
    paired_coord = ",".join(
        [
            f"{a}{b}"
            for a, b in zip(coordinates.split(",")[0::2], coordinates.split(",")[1::2])
        ]
    )
    return f"POLYGON({paired_coord})"


_describe_docs: Dict[str, Dict[str, Any]] = {}
_LEGACY_SEARCH_TERMS = [
    "startDate",
    "completionDate",
    "geometry",
    "uid",
    "published",
    "updated",
    "cloudCover",
    "productType",
    "processingLevel",
    "orbitNumber",
    "relativeOrbitNumber",
    "orbitDirection",
    "platform",
    "instrument",
    "polarisation",
    "polarisationChannels",
    "sensorMode",
    "swath",
    "timeliness",
    "status",
    "page",
    "exactCount",
    "sortParam",
    "sortOrder",
    "maxRecords",
]


def describe_collection(
    collection: str, proxies: Union[Dict[str, str], None] = None
) -> Dict[str, Any]:
    """
    Get a best-effort list of queryable keys for a collection in key-value pairs.
    """
    collections = resolve_stac_collections(collection, {})
    if not collections:
        return _legacy_term_descriptions()

    stac_collection = collections[0]
    cached = _describe_docs.get(stac_collection)
    if cached:
        return cached

    session = Credentials.make_session(
        None, False, Credentials.RETRIES, proxies)
    attempts = 0
    data: Dict[str, Any] = {}
    while attempts < 3:
        attempts += 1
        try:
            with session.get(f"{STAC_COLLECTIONS_URL}/{stac_collection}/queryables") as res:
                if res.status_code == 200:
                    data = res.json()
                    break
                if res.status_code in (400, 401, 403, 404):
                    return _legacy_term_descriptions()

                retry_after_seconds = _retry_after_seconds(
                    res.headers.get("Retry-After")
                )
                if retry_after_seconds is not None:
                    sleep(retry_after_seconds)
                else:
                    sleep(1 * (1 + (random() / 4)))
        except (RequestException, ValueError):
            sleep(1 * (1 + (random() / 4)))
    else:
        return _legacy_term_descriptions()

    properties = data.get("properties") or {}
    out = _legacy_term_descriptions()
    for name, metadata in properties.items():
        out[name] = {
            "pattern": None,
            "minInclusive": None,
            "maxInclusive": None,
            "title": metadata.get("description"),
        }

    _describe_docs[stac_collection] = out
    return out


def _validate_search_terms(
    collection: str,
    search_terms: Dict[str, Any],
    proxies: Union[Dict[str, str], None],
) -> None:
    description = describe_collection(collection, proxies=proxies)
    for key, value in search_terms.items():
        cfg = description.get(key)
        if cfg is None:
            raise AssertionError(
                f'search_term with name "{key}" was not found for collection.'
                + f" Available terms are: {', '.join(description.keys())}"
            )

        _valid_search_term(_serialize_search_term(value), cfg)


def _serialize_search_term(search_term: object) -> str:
    if isinstance(search_term, list):
        return ",".join(str(item) for item in search_term)

    if isinstance(search_term, datetime):
        return search_term.strftime("%Y-%m-%dT%H:%M:%SZ")

    if isinstance(search_term, date):
        return search_term.strftime("%Y-%m-%d")

    return str(search_term)


def _valid_search_term(search_term: str, cfg: Dict[str, str]) -> bool:
    return (
        _valid_match_pattern(search_term, cfg)
        and _valid_min_inclusive(search_term, cfg)
        and _valid_max_inclusive(search_term, cfg)
    )


def _valid_match_pattern(search_term: str, cfg: Dict[str, str]) -> bool:
    pattern = cfg.get("pattern")
    if not pattern:
        return True

    if re.match(pattern, search_term) is None:
        raise AssertionError(
            f"search_term {search_term} does not match pattern {pattern}"
        )
    return True


def _valid_min_inclusive(search_term: str, cfg: Dict[str, str]) -> bool:
    min_inclusive = cfg.get("minInclusive")
    if not min_inclusive:
        return True

    if int(search_term) < int(min_inclusive):
        raise AssertionError(
            f"search_term {search_term} is less than min_inclusive {min_inclusive}"
        )
    return True


def _valid_max_inclusive(search_term: str, cfg: Dict[str, str]) -> bool:
    max_inclusive = cfg.get("maxInclusive")
    if not max_inclusive:
        return True

    if int(search_term) > int(max_inclusive):
        raise AssertionError(
            f"search_term {search_term} is greater than max_inclusive {max_inclusive}"
        )
    return True


def _legacy_term_descriptions() -> Dict[str, Dict[str, Any]]:
    return {
        key: {
            "pattern": None,
            "minInclusive": None,
            "maxInclusive": None,
            "title": None,
        }
        for key in _LEGACY_SEARCH_TERMS
    }


def _retry_after_seconds(value: Optional[str]) -> Optional[float]:
    if not value:
        return None

    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass

    try:
        retry_after_date = parsedate_to_datetime(value)
        if retry_after_date.tzinfo is None:
            retry_after_date = retry_after_date.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return max(0.0, (retry_after_date - now).total_seconds())
    except (TypeError, ValueError):
        return None
