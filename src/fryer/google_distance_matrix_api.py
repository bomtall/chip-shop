import json
from collections.abc import Iterable
from typing import Literal

import pandas as pd
import requests

__all__ = [
    "call_google_distance_matrix_api",
    "extract_distance_from_response",
    "generate_google_distance_matrix_url",
]


travel_modes = Literal["driving", "walking", "bicycling", "transit"]
traffic_models = Literal["best_guess", "optimistic", "pessimistic"]
avoidances = Literal["tolls", "highways", "ferries", "indoor"]
transit_modes = Literal["bus", "subway", "train", "tram", "rail"]
transit_routing_preferences = Literal["less_walking", "fewer_transfers"]


def make_iterable(obj: Iterable | None) -> Iterable:
    if not isinstance(obj, str) and isinstance(obj, Iterable):
        return obj
    if obj:
        return [obj]
    return []


def generate_google_distance_matrix_url(
    key: str,
    origin_postcode: str,
    destination_postcode: str,
    arrival_timestamp: pd.Timestamp | None = None,
    departure_timestamp: pd.Timestamp | None = None,
    avoids: Iterable[avoidances] | avoidances | None = None,
    mode: travel_modes | None = "driving",
    traffic_model: traffic_models | None = "best_guess",
    transit_modes: Iterable[transit_modes] | transit_modes | None = None,
    transit_routing_preference: transit_routing_preferences | None = None,
) -> str:
    """Generate a URL for the Google Distance Matrix API.
    Info https://developers.google.com/maps/documentation/distance-matrix/distance-matrix.
    """
    avoid = "|".join(make_iterable(avoids))
    transit_mode = "|".join(make_iterable(transit_modes))
    arrival_time = (
        str(int(arrival_timestamp.timestamp())) if arrival_timestamp else None
    )
    departure_time = (
        str(int(departure_timestamp.timestamp())) if departure_timestamp else None
    )

    return f"https://maps.googleapis.com/maps/api/distancematrix/json?{
        'origins='
        + origin_postcode
        + '&destinations='
        + destination_postcode
        + '&units=metric'
        + '&language=en-GB'
        + f'&{key=!s}'
        + f'&{mode=!s}'
        + (f'&{transit_mode=!s}' if mode == 'transit' and transit_mode else '')
        + (
            f'&{transit_routing_preference=!s}'
            if mode == 'transit' and transit_routing_preference
            else ''
        )
        + (f'&{avoid=!s}' if avoid else '')
        + (f'&{arrival_time=!s}' if arrival_time else '')
        + (f'&{departure_time=!s}' if departure_time and not arrival_time else '')
        + (f'&{traffic_model=!s}' if mode == 'driving' and departure_time else '')
    }"


def call_google_distance_matrix_api(url: str) -> dict | None:
    response = requests.get(url, headers={}, data={}, timeout=30)
    ok = 200
    if response.status_code == ok:
        return json.loads(response.text)
    return None


def extract_distance_from_response(data: dict) -> float:
    distance = data["rows"][0]["elements"][0]["distance"]["text"]
    return float(distance[0:-2].replace(" ", ""))
