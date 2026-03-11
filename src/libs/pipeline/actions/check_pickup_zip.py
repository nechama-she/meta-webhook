"""Action: flag whether the pickup zip is in the service area.

Zips in NC, SC, GA, FL, TN are OUTSIDE the service area (leads get sold).
All other zips are IN the service area.
"""

# Zip code ranges by state
_RANGES = [
    (27000, 28999),  # NC
    (29000, 29999),  # SC
    (30000, 31999),  # GA
    (39800, 39999),  # GA (secondary)
    (32000, 34999),  # FL
    (37000, 38599),  # TN
]


def check_pickup_zip(data: dict) -> dict:
    """Set data['in_service_area'] based on the pickup zip."""
    raw = data.get("pickup_zip", data.get("ozip", ""))
    try:
        zip_int = int(raw)
        data["in_service_area"] = not any(lo <= zip_int <= hi for lo, hi in _RANGES)
    except (ValueError, TypeError):
        data["in_service_area"] = False
    return data
