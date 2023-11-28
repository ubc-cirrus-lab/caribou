import os

import googlemaps

GOOGLE_MAPS_API_KEY = os.environ.get("MULTI_X_SERVERLESS_GOOGLE_API_KEY")


class Location:
    def __init__(self, latitude: float, longitude: float):
        self.latitude = latitude
        self.longitude = longitude

    def get_latitude(self) -> float:
        return self.latitude

    def get_longitude(self) -> float:
        return self.longitude

    def __str__(self) -> str:
        return f"({self.latitude}, {self.longitude})"

    def __repr__(self) -> str:
        return f"Location({self.latitude}, {self.longitude})"


def get_location(location_name: str) -> Location:
    gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)

    if location_name == "Columbus":
        location_name = "Columbus, Ohio"  # Somehow Google Maps doesn't know where Columbus, OH is
    geocode_result = gmaps.geocode(location_name)
    if geocode_result:
        lat = geocode_result[0]["geometry"]["location"]["lat"]
        lng = geocode_result[0]["geometry"]["location"]["lng"]
    else:
        raise ValueError(f"Could not find location {location_name}")
    return Location(lat, lng)
