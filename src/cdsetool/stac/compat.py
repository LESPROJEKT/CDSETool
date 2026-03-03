"""Map CDSE STAC items to the legacy OpenSearch-like feature shape."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional


def stac_item_to_cdse_feature(
    item: Dict[str, Any], requested_collection: str
) -> Dict[str, Any]:
    """Convert a STAC item to the feature format expected by legacy callers."""
    properties = dict(item.get("properties") or {})
    geometry = item.get("geometry")
    assets = item.get("assets") or {}

    stac_collection = str(item.get("collection") or "")
    legacy_collection = _legacy_collection_name(stac_collection, requested_collection)

    download_asset = _select_download_asset(assets)
    download_url = _normalize_download_href((download_asset or {}).get("href"))

    title = _derive_title(item, properties, download_asset, legacy_collection)
    centroid = properties.get("centroid") or _build_centroid(geometry)
    thumbnail_url = _extract_thumbnail_url(assets)

    properties["title"] = title
    properties.setdefault("collection", legacy_collection)
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
    properties.setdefault(
        "sensorMode",
        properties.get("sar:instrument_mode") or properties.get("eopf:instrument_mode"),
    )
    properties.setdefault("polarisation", _legacy_polarisation(properties))

    if "productType" not in properties:
        product_type = properties.get("product:type")
        if isinstance(product_type, str) and product_type.endswith("-COG"):
            product_type = product_type.removesuffix("-COG")
        properties["productType"] = product_type

    if "processingLevel" not in properties:
        properties["processingLevel"] = (
            properties.get("product:type") or properties.get("processing:level")
        )

    if centroid:
        properties["centroid"] = centroid

    if thumbnail_url:
        properties.setdefault("thumbnail", thumbnail_url)
        properties.setdefault("quicklook", thumbnail_url)

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


def _legacy_collection_name(stac_collection: str, requested_collection: str) -> str:
    if stac_collection.startswith("sentinel-1"):
        return "SENTINEL-1"
    if stac_collection.startswith("sentinel-2"):
        return "SENTINEL-2"
    if stac_collection.startswith("sentinel-3"):
        return "SENTINEL-3"
    if stac_collection.startswith("cop-dem-"):
        return "COP-DEM"

    if requested_collection.startswith("Sentinel"):
        return requested_collection.replace("Sentinel", "SENTINEL-")

    return requested_collection.upper()


def _select_download_asset(assets: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for key in ("Product", "product"):
        asset = assets.get(key)
        if isinstance(asset, dict) and asset.get("href"):
            return asset

    for asset in assets.values():
        if not isinstance(asset, dict):
            continue
        href = str(asset.get("href") or "")
        roles = asset.get("roles") or []
        asset_type = str(asset.get("type") or "")
        if (
            "application/zip" in asset_type
            or "archive" in roles
            or "/Products(" in href
        ):
            return asset

    data_asset = assets.get("data")
    if isinstance(data_asset, dict) and data_asset.get("href"):
        return data_asset

    for asset in assets.values():
        if isinstance(asset, dict) and asset.get("href"):
            return asset

    return None


def _normalize_download_href(href: Optional[str]) -> Optional[str]:
    if not href:
        return None

    if href.startswith("s3://eodata/"):
        return href.replace("s3://eodata/", "https://eodata.dataspace.copernicus.eu/")

    return href


def _derive_title(
    item: Dict[str, Any],
    properties: Dict[str, Any],
    download_asset: Optional[Dict[str, Any]],
    legacy_collection: str,
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
    if (
        legacy_collection in ("SENTINEL-1", "SENTINEL-2")
        and title
        and not title.endswith(".SAFE")
    ):
        return f"{title}.SAFE"
    return title


def _build_centroid(geometry: Any) -> Optional[Dict[str, Any]]:
    if not geometry:
        return None
    try:
        from shapely.geometry import shape

        centroid = shape(geometry).centroid
        return {"type": "Point", "coordinates": [centroid.x, centroid.y]}
    except Exception:
        return None


def _extract_thumbnail_url(assets: Dict[str, Dict[str, Any]]) -> Optional[str]:
    thumbnail = assets.get("thumbnail")
    if thumbnail and thumbnail.get("href"):
        return str(thumbnail["href"])
    return None


def _legacy_polarisation(properties: Dict[str, Any]) -> Optional[str]:
    polarizations = properties.get("sar:polarizations")
    if isinstance(polarizations, list):
        return "&".join(str(item).upper() for item in polarizations if item)
    if isinstance(polarizations, str):
        return polarizations.upper()
    return None
