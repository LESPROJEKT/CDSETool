"""STAC translation and compatibility helpers used by :mod:`cdsetool.query`."""

from cdsetool.stac.compat import stac_item_to_cdse_feature
from cdsetool.stac.translate import (
    STAC_COLLECTIONS_URL,
    STAC_SEARCH_URL,
    build_stac_search_payload,
    resolve_stac_collections,
)

__all__ = [
    "STAC_COLLECTIONS_URL",
    "STAC_SEARCH_URL",
    "build_stac_search_payload",
    "resolve_stac_collections",
    "stac_item_to_cdse_feature",
]
