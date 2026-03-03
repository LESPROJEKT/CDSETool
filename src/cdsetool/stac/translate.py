"""Translate legacy OpenSearch-style query terms into STAC search payloads."""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union
from urllib.parse import unquote

STAC_API_URL = "https://stac.dataspace.copernicus.eu/v1"
STAC_SEARCH_URL = f"{STAC_API_URL}/search"
STAC_COLLECTIONS_URL = f"{STAC_API_URL}/collections"


def build_stac_search_payload(
    collection: str,
    search_terms: Dict[str, Any],
) -> Dict[str, Any]:
    """Build a STAC ``/search`` payload from legacy search term names."""
    terms = dict(search_terms or {})
    payload: Dict[str, Any] = {
        "collections": resolve_stac_collections(collection, terms),
        "limit": _parse_limit(terms.get("maxRecords", 2000)),
    }

    uid = terms.get("uid")
    if uid:
        if isinstance(uid, (list, tuple)):
            payload["ids"] = [str(item) for item in uid if str(item).strip()]
        else:
            payload["ids"] = [str(uid)]

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

    # CDSE uses specific product:type values; skip filter when legacy/short form is requested.
    # - sentinel-1-grd: values are IW_GRDH_1S, EW_GRDH_1S, not "GRD".
    # - cop-dem-glo-*-cog: values are DGE_30-COG / DGE_90-COG, and product:type is not in queryables.
    collections = payload.get("collections") or []
    product_type = terms.get("productType")
    if product_type:
        pt_str = str(product_type).strip()
        pt_upper = pt_str.upper()
        skip_s1_grd = "sentinel-1-grd" in collections and pt_upper == "GRD"
        skip_copdem = any(
            c.startswith("cop-dem-glo-") and c.endswith("-cog") for c in collections
        ) and pt_upper in ("DGE_30", "DGE_90")
        if not skip_s1_grd and not skip_copdem:
            query["product:type"] = {"eq": pt_str}

    processing_level = terms.get("processingLevel")
    if processing_level:
        processing_level_str = str(processing_level)
        if processing_level_str.upper().startswith("S2MSI"):
            query["product:type"] = {"eq": processing_level_str}
        else:
            query["processing:level"] = {"eq": processing_level_str}

    cloud_cover = terms.get("cloudCover")
    if cloud_cover is not None:
        cloud_cover_query = _build_range_query(cloud_cover)
        if cloud_cover_query:
            query["eo:cloud_cover"] = cloud_cover_query

    sensor_mode = terms.get("sensorMode")
    if sensor_mode:
        query["sar:instrument_mode"] = {"eq": str(sensor_mode)}

    relative_orbit = terms.get("relativeOrbitNumber")
    if relative_orbit is None:
        relative_orbit = terms.get("orbitNumber")
    if relative_orbit is not None:
        relative_orbit_query = _build_range_query(relative_orbit)
        if relative_orbit_query:
            query["sat:relative_orbit"] = relative_orbit_query

    orbit_direction = terms.get("orbitDirection")
    if orbit_direction:
        query["sat:orbit_state"] = {"eq": str(orbit_direction).lower()}

    platform = terms.get("platform")
    if platform:
        query["platform"] = {"eq": str(platform)}

    timeliness = terms.get("timeliness")
    if timeliness:
        query["product:timeliness"] = {"eq": str(timeliness)}

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


def resolve_stac_collections(collection: str, terms: Dict[str, Any]) -> List[str]:
    """Map legacy collection names to STAC collection ids."""
    collection_l = collection.lower()

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

        # Most existing callers expect L2A-compatible products.
        return ["sentinel-2-l2a"]

    if collection == "Sentinel1":
        return ["sentinel-1-grd"]

    if collection == "COP-DEM":
        product_type = str(terms.get("productType") or "").upper()
        if "90" in product_type:
            return ["cop-dem-glo-90-dged-cog"]
        return ["cop-dem-glo-30-dged-cog"]

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

        try:
            from shapely import wkt as shapely_wkt
            from shapely.geometry import mapping
        except ImportError as error:
            raise RuntimeError(
                "WKT geometry conversion requires shapely to be installed."
            ) from error

        geojson_geom = mapping(shapely_wkt.loads(geometry))
        if (
            geojson_geom.get("type") == "GeometryCollection"
            and isinstance(geojson_geom.get("geometries"), list)
            and len(geojson_geom["geometries"]) == 1
        ):
            return geojson_geom["geometries"][0]

        return geojson_geom

    raise ValueError(f"Unsupported geometry value: {type(geometry)}")


def _serialize_interval_bound(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")

    if isinstance(value, date):
        return value.strftime("%Y-%m-%dT00:00:00Z")

    value_str = str(value).strip()
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
    if order in ("descending", "desc", "-1"):
        return "desc"
    return "asc"


def _build_range_query(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return {
            "gte": _to_number(value[0]),
            "lte": _to_number(value[1]),
        }

    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        parsed_range = _parse_bracket_range(value)
        if parsed_range is not None:
            return parsed_range
        return {"eq": _to_number(value)}

    if isinstance(value, (int, float)):
        return {"eq": value}

    return None


def _parse_bracket_range(value: str) -> Optional[Dict[str, Any]]:
    if not (value.startswith("[") and value.endswith("]") and "," in value):
        return None

    left, right = value[1:-1].split(",", 1)
    range_query: Dict[str, Any] = {}
    left = left.strip()
    right = right.strip()

    if left and left != "*":
        range_query["gte"] = _to_number(left)
    if right and right != "*":
        range_query["lte"] = _to_number(right)

    return range_query or None


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
        return [item.strip() for item in decoded.split("&") if item.strip()]

    return []
