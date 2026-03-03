"""
Query the Copernicus Data Space Ecosystem STAC API through an OpenSearch-like interface.

This module keeps the legacy `query_features(...)` contract used across the project,
but translates requests to STAC `/search` and maps STAC items back to the feature
shape expected by existing code.
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
from urllib.parse import unquote
import json
import os
import re
from random import random
from time import sleep

from requests.exceptions import ChunkedEncodingError, RequestException
from shapely import wkt as shapely_wkt
from shapely.geometry import mapping, shape
from urllib3.exceptions import ProtocolError

from cdsetool.credentials import Credentials
from cdsetool.logger import NoopLogger

STAC_API_URL = "https://stac.dataspace.copernicus.eu/v1"
STAC_SEARCH_URL = f"{STAC_API_URL}/search"
STAC_COLLECTIONS_URL = f"{STAC_API_URL}/collections"


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
    An iterator over the features matching the search terms

    Queries the API in batches (default: 50) features, and returns them one by one.
    Queries the next batch when the current batch is exhausted.
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
        self.proxies = proxies
        self.log = (options or {}).get("logger") or NoopLogger()
        self.collection = collection
        self.next_request = _initial_request(collection, search_terms)
        # Eagerly fetch first page to preserve callers that inspect `.features` directly.
        self.__fetch_features()

    def __iter__(self):
        return _FeatureIterator(self)

    def __len__(self) -> int:
        if self.total_results >= 0:
            return self.total_results

        while self.next_request is not None:
            self.__fetch_features()

        return len(self.features)

    def __getitem__(self, index):
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
        while attempts < 3:
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
                        if response.status_code in [400, 401, 403, 404]:
                            response.raise_for_status()
                        sleep(60 * (1 + (random() / 4)))
                        continue
                    res = response.json()
                    mapped_features = [
                        _stac_item_to_feature(item, self.collection)
                        for item in (res.get("features") or [])
                    ]
                    self.features += mapped_features

                    total_results = res.get("numberMatched")
                    if total_results is None:
                        context = res.get("context") or {}
                        total_results = context.get("matched")

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
            ) as e:
                self.log.warning(e)
                continue

        raise RuntimeError(
            f"Failed to query STAC after {attempts} attempts for {self.collection}"
        )

    def __set_next_request(self, res) -> None:
        links = res.get("links") or []
        next_link = next((link for link in links if link.get("rel") == "next"), {})
        href = next_link.get("href")
        if not href:
            self.next_request = None
            return

        method = (next_link.get("method") or "GET").upper()
        request: Dict[str, Any] = {"method": method, "url": href}
        if method == "POST":
            request["json"] = next_link.get("body") or {}
        self.next_request = request


def _initial_request(collection: str, search_terms: Dict[str, Any]) -> Dict[str, Any]:
    payload = _to_stac_payload(collection, search_terms)
    return {"method": "POST", "url": STAC_SEARCH_URL, "json": payload}


def _to_stac_payload(collection: str, search_terms: Dict[str, Any]) -> Dict[str, Any]:
    terms = dict(search_terms or {})
    payload: Dict[str, Any] = {
        "collections": _resolve_collections(collection, terms),
        "limit": _parse_limit(terms.get("maxRecords", 2000)),
    }

    start_date = terms.get("startDate")
    completion_date = terms.get("completionDate")
    if start_date or completion_date:
        start_str = _serialize_interval_bound(start_date) if start_date else ".."
        end_str = _serialize_interval_bound(completion_date) if completion_date else ".."
        payload["datetime"] = f"{start_str}/{end_str}"

    if terms.get("geometry"):
        payload["intersects"] = _to_geojson_geometry(terms["geometry"])

    sort_field = _map_sort_field(terms.get("sortParam"))
    if sort_field:
        payload["sortby"] = [
            {
                "field": sort_field,
                "direction": _map_sort_direction(terms.get("sortOrder")),
            }
        ]

    query: Dict[str, Any] = {}

    product_type = terms.get("productType")
    if product_type:
        query["product:type"] = {"eq": str(product_type)}

    processing_level = terms.get("processingLevel")
    if processing_level:
        processing_level_str = str(processing_level)
        if processing_level_str.upper().startswith("S2MSI"):
            query["product:type"] = {"eq": processing_level_str}
        else:
            query["processing:level"] = {"eq": processing_level_str}

    cloud_cover = terms.get("cloudCover")
    if cloud_cover is not None:
        cc_query = _build_range_query(cloud_cover)
        if cc_query:
            query["eo:cloud_cover"] = cc_query

    sensor_mode = terms.get("sensorMode")
    if sensor_mode:
        query["sar:instrument_mode"] = {"eq": str(sensor_mode)}

    polarisation = terms.get("polarisation")
    if polarisation is None:
        polarisation = terms.get("polarisationChannels")
    if polarisation:
        polarizations = _parse_polarizations(polarisation)
        if polarizations:
            query["sar:polarizations"] = {"eq": polarizations}

    if query:
        payload["query"] = query

    return payload


def _resolve_collections(collection: str, terms: Dict[str, Any]) -> List[str]:
    collection_l = collection.lower()

    # Already a STAC collection id
    if collection_l.startswith("sentinel-") or collection_l.startswith("cop-dem-"):
        return [collection_l]

    if collection == "Sentinel2":
        product_type = str(terms.get("productType") or "").upper()
        processing_level = str(terms.get("processingLevel") or "").upper()
        marker = f"{product_type} {processing_level}"

        if "1C" in marker:
            return ["sentinel-2-l1c"]
        if "2A" in marker:
            return ["sentinel-2-l2a"]

        # Most existing code expects L2A bands, so default to L2A.
        return ["sentinel-2-l2a"]

    if collection == "Sentinel1":
        return ["sentinel-1-grd"]

    if collection == "COP-DEM":
        product_type = str(terms.get("productType") or "").upper()
        if "90" in product_type:
            return ["cop-dem-glo-90-dged-cog"]
        return ["cop-dem-glo-30-dged-cog"]

    # Generic fallback for unknown custom collections
    return [collection_l]


def _parse_limit(value: Any) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        return 2000
    return max(1, min(limit, 2000))


def _to_geojson_geometry(geometry: Any) -> Dict[str, Any]:
    if isinstance(geometry, dict):
        if geometry.get("type") == "Feature":
            return geometry.get("geometry") or {}
        return geometry

    if isinstance(geometry, str):
        geometry = geometry.strip()
        if geometry.startswith("{"):
            loaded = json.loads(geometry)
            if loaded.get("type") == "Feature":
                return loaded.get("geometry") or {}
            return loaded
        geom = mapping(shapely_wkt.loads(geometry))
        # Some STAC deployments are stricter with intersects geometry. If we have
        # a single-part GeometryCollection, unwrap it to the concrete geometry.
        if (
            geom.get("type") == "GeometryCollection"
            and isinstance(geom.get("geometries"), list)
            and len(geom["geometries"]) == 1
        ):
            return geom["geometries"][0]
        return geom

    raise ValueError(f"Unsupported geometry value: {type(geometry)}")


def _serialize_interval_bound(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")

    if isinstance(value, date):
        return value.strftime("%Y-%m-%dT00:00:00Z")

    value_str = str(value).strip()
    # Legacy callers often pass plain YYYY-MM-DD; STAC requires full datetime.
    if re.match(r"^\d{4}-\d{2}-\d{2}$", value_str):
        return f"{value_str}T00:00:00Z"

    return value_str


def _map_sort_field(sort_param: Any) -> Optional[str]:
    if not sort_param:
        return None

    mapping_map = {
        "startDate": "datetime",
        "completionDate": "end_datetime",
        "published": "published",
        "updated": "updated",
        "cloudCover": "eo:cloud_cover",
    }
    return mapping_map.get(str(sort_param), str(sort_param))


def _map_sort_direction(sort_order: Any) -> str:
    order = str(sort_order or "asc").lower()
    if order in ["descending", "desc", "-1"]:
        return "desc"
    return "asc"


def _build_range_query(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return {"gte": _to_number(value[0]), "lte": _to_number(value[1])}

    if isinstance(value, str):
        v = value.strip()
        if v.startswith("[") and v.endswith("]") and "," in v:
            left, right = v[1:-1].split(",", 1)
            return {"gte": _to_number(left.strip()), "lte": _to_number(right.strip())}
        return {"eq": _to_number(v)}

    if isinstance(value, (int, float)):
        return {"eq": value}

    return None


def _to_number(value: Any) -> Union[int, float, str]:
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value
    return str(value)


def _parse_polarizations(value: Any) -> List[str]:
    if isinstance(value, (list, tuple)):
        return [str(v).upper() for v in value if str(v).strip()]

    if isinstance(value, str):
        decoded = unquote(value).upper().replace(",", "&").replace("|", "&")
        return [item for item in decoded.split("&") if item.strip()]

    return []


def _stac_item_to_feature(item: Dict[str, Any], collection: str) -> Dict[str, Any]:
    properties = dict(item.get("properties") or {})
    geometry = item.get("geometry")
    assets = item.get("assets") or {}

    download_asset = _select_download_asset(assets)
    download_url = (download_asset or {}).get("href")

    title = _derive_title(item, properties, download_asset, collection)
    centroid = properties.get("centroid") or _build_centroid(geometry)

    properties["title"] = title
    properties["startDate"] = (
        properties.get("start_datetime") or properties.get("datetime")
    )
    properties["completionDate"] = (
        properties.get("end_datetime") or properties.get("datetime")
    )
    properties["published"] = properties.get("published") or properties.get("created")
    properties["updated"] = properties.get("updated")
    properties["relativeOrbitNumber"] = properties.get("sat:relative_orbit")
    orbit_state = properties.get("sat:orbit_state")
    properties["orbitDirection"] = (
        str(orbit_state).upper() if orbit_state is not None else None
    )
    properties["cloudCover"] = properties.get("eo:cloud_cover")
    if "productType" not in properties:
        properties["productType"] = properties.get("product:type")
    if "processingLevel" not in properties:
        properties["processingLevel"] = (
            properties.get("product:type") or properties.get("processing:level")
        )
    if centroid:
        properties["centroid"] = centroid

    properties["services"] = {"download": {"url": download_url}}

    feature: Dict[str, Any] = {
        "type": "Feature",
        "id": item.get("id"),
        "geometry": geometry,
        "properties": properties,
    }

    if item.get("bbox") is not None:
        feature["bbox"] = item["bbox"]

    return feature


def _select_download_asset(assets: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for key in ("Product", "product"):
        asset = assets.get(key)
        if asset and asset.get("href"):
            return asset

    for asset in assets.values():
        href = str(asset.get("href") or "")
        roles = asset.get("roles") or []
        asset_type = str(asset.get("type") or "")
        if (
            "application/zip" in asset_type
            or "archive" in roles
            or "/Products(" in href
        ):
            return asset

    return None


def _derive_title(
    item: Dict[str, Any],
    properties: Dict[str, Any],
    download_asset: Optional[Dict[str, Any]],
    collection: str,
) -> str:
    local_path = None
    if download_asset:
        local_path = download_asset.get("file:local_path")

    if local_path:
        title = os.path.basename(local_path)
        if title.endswith(".zip"):
            return title[:-4]
        return title

    title = str(properties.get("title") or item.get("id") or "")
    if collection in ["Sentinel1", "Sentinel2"] and title and not title.endswith(".SAFE"):
        return f"{title}.SAFE"
    return title


def _build_centroid(geometry: Any) -> Optional[Dict[str, Any]]:
    if not geometry:
        return None
    try:
        centroid = shape(geometry).centroid
        return {"type": "Point", "coordinates": [centroid.x, centroid.y]}
    except Exception:
        return None



def query_features(
    collection: str,
    search_terms: Dict[str, Any],
    proxies: Union[Dict[str, str], None] = None,
    options: Union[Dict[str, Any], None] = None,
) -> FeatureQuery:
    """
    Returns an iterator over the features matching the search terms
    """
    return FeatureQuery(
        collection, {"maxRecords": 2000, **search_terms}, proxies, options
    )


def shape_to_wkt(shape: str) -> str:
    """
    Convert a shapefile to a WKT string
    """
    import geopandas as gpd  # Lazy import to avoid hard dependency for query_features()

    coordinates = list(gpd.read_file(shape).geometry[0].exterior.coords)
    return (
        "POLYGON(("
        + ", ".join(" ".join(map(str, coord)) for coord in coordinates)
        + "))"
    )


def geojson_to_wkt(geojson_in: Union[str, Dict]) -> str:
    """
    Convert a geojson geometry to a WKT string
    """
    geojson = json.loads(geojson_in) if isinstance(geojson_in, str) else geojson_in

    if geojson.get("type") == "Feature":
        geojson = geojson["geometry"]
    elif geojson.get("type") == "FeatureCollection" and len(geojson["features"]) == 1:
        geojson = geojson["features"][0]["geometry"]

    coordinates = str(
        tuple(item for sublist in geojson["coordinates"][0] for item in sublist)
    )
    paired_coord = ",".join(
        [
            f"{a}{b}"
            for a, b in zip(coordinates.split(",")[0::2], coordinates.split(",")[1::2])
        ]
    )
    return f"POLYGON({paired_coord})"


def describe_collection(
    collection: str, proxies: Union[Dict[str, str], None] = None
) -> Dict[str, Any]:
    """
    Get a best-effort list of queryable keys for a collection in key-value pairs.
    """
    collections = _resolve_collections(collection, {})
    if not collections:
        return {}

    session = Credentials.make_session(None, False, Credentials.RETRIES, proxies)
    stac_collection = collections[0]
    with session.get(f"{STAC_COLLECTIONS_URL}/{stac_collection}/queryables") as res:
        if res.status_code != 200:
            return {}
        data = res.json()

    properties = data.get("properties") or {}
    out = {}
    for name, metadata in properties.items():
        out[name] = {
            "pattern": None,
            "minInclusive": None,
            "maxInclusive": None,
            "title": metadata.get("description"),
        }

    # Legacy aliases expected by existing callers.
    legacy_keys = [
        "startDate",
        "completionDate",
        "geometry",
        "cloudCover",
        "productType",
        "processingLevel",
        "polarisation",
        "polarisationChannels",
        "sensorMode",
        "sortParam",
        "sortOrder",
        "maxRecords",
    ]
    for key in legacy_keys:
        out.setdefault(
            key,
            {
                "pattern": None,
                "minInclusive": None,
                "maxInclusive": None,
                "title": None,
            },
        )

    return out
