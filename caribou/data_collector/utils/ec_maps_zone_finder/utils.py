import asyncio
import json
import os
from typing import Any

from caribou.data_collector.utils.ec_maps_zone_finder import turf

MAX_NEAREST_ZONE_DISTANCE_KM = 10.0

GEO_GENERATED_FILE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "geo.generated.json"))


class GeoFeatureLoader:
    _feature_future: asyncio.Future | None = None
    _load_lock = asyncio.Lock()

    async def _actual_load_data_internal(self) -> dict[str, Any]:
        """Helper function to perform the actual file reading and parsing."""
        loop = asyncio.get_running_loop()
        try:

            def read_and_parse() -> dict[str, Any]:
                with open(GEO_GENERATED_FILE_PATH, "r", encoding="utf-8") as f:
                    data_str = f.read()
                return json.loads(data_str)

            # Ensure GEO_GENERATED_FILE_PATH is accessible or passed appropriately
            parsed_data = await loop.run_in_executor(None, read_and_parse)
            return parsed_data
        except FileNotFoundError:
            print(f"Error: Geometry file not found at {GEO_GENERATED_FILE_PATH}. Please run data generation script.")
            raise
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON from {GEO_GENERATED_FILE_PATH}: {e}")
            raise
        except Exception as e:  # Catch other unexpected errors during loading
            print(f"An unexpected error occurred while loading geo features: {e}")
            raise

    async def get_features(self) -> dict[str, Any]:
        if GeoFeatureLoader._feature_future is None:
            async with GeoFeatureLoader._load_lock:
                # Double-check after acquiring the lock (double-checked locking pattern)
                if GeoFeatureLoader._feature_future is None:
                    loop = asyncio.get_running_loop()
                    current_future = loop.create_future()
                    GeoFeatureLoader._feature_future = current_future  # Cache the future immediately
                    try:
                        data = await self._actual_load_data_internal()
                        current_future.set_result(data)
                    except Exception as e:
                        current_future.set_exception(e)
                        # Nullify the cache on load failure so it can be retried
                        GeoFeatureLoader._feature_future = None
                        raise  # Re-raise the exception to the caller

        return await GeoFeatureLoader._feature_future


# Global instance or function to access it easily
_geo_loader_instance = GeoFeatureLoader()


async def load_geometry_features_cached() -> dict[str, Any]:  # This now uses the class instance
    return await _geo_loader_instance.get_features()


_load_lock = asyncio.Lock()


async def _actual_load_data() -> dict[str, Any]:
    """Helper function to perform the actual file reading and parsing."""
    loop = asyncio.get_running_loop()
    try:

        def read_and_parse() -> dict[str, Any]:
            with open(GEO_GENERATED_FILE_PATH, "r", encoding="utf-8") as f:
                data_str = f.read()
            return json.loads(data_str)

        parsed_data = await loop.run_in_executor(None, read_and_parse)

        return parsed_data
    except FileNotFoundError:
        print(f"Error: Geometry file not found at {GEO_GENERATED_FILE_PATH}. Please run data generation script.")
        raise
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON from {GEO_GENERATED_FILE_PATH}: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred while loading geo features: {e}")
        raise


def get_nearest_zone(
    potential_zones: list[str], zone_to_lines: dict[str, list[Any]], target_point: Any
) -> dict[str, Any] | None:
    """
    Finds the nearest zone to the target point from a list of potential zones.

    Args:
        potential_zones (list): A list of zone keys (strings) to consider.
        zone_to_lines (dict): A dictionary mapping zone keys to lists of lines (LineString features or coordinates).
        target_point: A GeoJSON Point feature or coordinates for the target location.

    Returns:
        A dictionary {'zoneName': str, 'distance': float} or None if no suitable zone is found.
    """
    result = None

    for zone_key in potential_zones:
        lines = zone_to_lines.get(zone_key, [])  # Use .get for safety if zoneKey might be missing

        for line in lines:
            distance_val = turf.point_to_line_distance(target_point, line)

            current_result = {
                "zoneName": zone_key,
                "distance": distance_val,
            }

            if result is None:
                result = current_result
            elif current_result["distance"] < result["distance"]:
                result = current_result

    return result


async def reverse_geocode(lon: float, lat: float) -> str | None:
    """
    Performs reverse geocoding for the given longitude and latitude.

    Args:
        lon (float): Longitude of the point.
        lat (float): Latitude of the point.

    Returns:
        str: The name of the zone, or None if no zone is found.
    """
    loaded_data = await load_geometry_features_cached()
    convexhulls = loaded_data.get("convexhulls", [])
    zone_to_geometry_features = loaded_data.get("zoneToGeometryFeatures", {})
    zone_to_lines = loaded_data.get("zoneToLines", {})

    target_point = turf.point([lon, lat])

    convex_hits = []
    if convexhulls:
        for _, feature_item in enumerate(convexhulls):
            if not isinstance(feature_item, dict):
                continue

            if not (
                feature_item.get("type") == "Feature"
                and isinstance(feature_item.get("geometry"), dict)
                and feature_item["geometry"].get("type") == "Polygon"
                and "coordinates" in feature_item["geometry"]
            ):
                continue

            if turf.boolean_point_in_polygon(target_point, feature_item):
                convex_hits.append(feature_item)

    convex_zone_names = [
        feature["properties"]["zoneName"]
        for feature in convex_hits
        if "properties" in feature and "zoneName" in feature["properties"]
    ]

    precise_hit_zone_names = []
    for zone_key in convex_zone_names:
        geometries_for_zone = zone_to_geometry_features.get(zone_key, [])
        if any(turf.boolean_point_in_polygon(target_point, poly) for poly in geometries_for_zone):
            precise_hit_zone_names.append(zone_key)

    initial_filtering_zones = set(precise_hit_zone_names)

    hit = None
    if len(initial_filtering_zones) == 1:
        hit = list(initial_filtering_zones)[0]

    if hit is None:
        if len(initial_filtering_zones) == 0:
            potential_zones_for_nearest = list(zone_to_lines.keys())
        else:
            potential_zones_for_nearest = list(initial_filtering_zones)

        nearest_result = get_nearest_zone(potential_zones_for_nearest, zone_to_lines, target_point)

        if nearest_result and nearest_result["distance"] < MAX_NEAREST_ZONE_DISTANCE_KM:
            hit = nearest_result["zoneName"]

    return hit
