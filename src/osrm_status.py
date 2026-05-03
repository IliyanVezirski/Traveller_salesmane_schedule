"""Small OSRM availability check used by CLI/GUI/release smoke tests."""

from __future__ import annotations

from typing import Any

import requests


def check_osrm_status(osrm_url: str) -> dict[str, Any]:
    """Return a user-friendly OSRM status dict.

    The check uses OSRM's nearest service with a fixed coordinate. Failure is
    reported as unavailable; callers should keep haversine fallback enabled.
    """
    url = str(osrm_url or "").strip().rstrip("/")
    if not url:
        return {"available": False, "message": "OSRM URL is empty.", "url": osrm_url}

    endpoint = f"{url}/nearest/v1/driving/23.3219,42.6977"
    try:
        response = requests.get(endpoint, timeout=3)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return {"available": False, "message": f"OSRM is not available: {exc}", "url": url}

    if payload.get("code") == "Ok":
        return {"available": True, "message": "OSRM is available.", "url": url}
    return {
        "available": False,
        "message": f"OSRM returned {payload.get('code')}: {payload.get('message', 'no message')}",
        "url": url,
    }
