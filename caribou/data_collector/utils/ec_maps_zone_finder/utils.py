import asyncio
import json
import os

import caribou.data_collector.utils.ec_maps_zone_finder.turf as turf

MAX_NEAREST_ZONE_DISTANCE_KM = 10.0

GEO_GENERATED_FILE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), 'geo.generated.json'))

_geo_feature_future = None
_load_lock = asyncio.Lock()

async def _actual_load_data():
    """Helper function to perform the actual file reading and parsing."""
    loop = asyncio.get_running_loop()
    try:
        def read_and_parse():
            with open(GEO_GENERATED_FILE_PATH, 'r', encoding='utf-8') as f:
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


async def load_geometry_features():
    """
    Loads the geometry features from disk for reverse geocoding.
    Caches the result for subsequent calls.

    Returns:
        A dictionary containing 'convexhulls', 'zoneToGeometryFeatures', and 'zoneToLines'.
    """
    global _geo_feature_future

    if _geo_feature_future is None:
        async with _load_lock:
            # Double-check idiom after acquiring the lock
            if _geo_feature_future is None:
                # Create a future and immediately assign it to the global cache.
                # This ensures that concurrent callers await the same future.
                loop = asyncio.get_running_loop()
                current_future = loop.create_future()
                _geo_feature_future = current_future
                try:
                    # The JS comment indicated the file is huge and blocks the main thread.
                    # The actual loading logic is encapsulated in _actual_load_data.
                    data = await _actual_load_data()
                    current_future.set_result(data)
                except Exception as e:
                    print(f"Error loading geo features, please run \"yarn generate-geo-file\" or equivalent: {e}")
                    current_future.set_exception(e)

    return await _geo_feature_future


def get_nearest_zone(potential_zones, zone_to_lines, target_point):
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
            # Assuming turf.pointToLineDistance expects targetPoint and line (e.g., list of coords)
            # The JS `turf.pointToLineDistance` itself handles `options` for units,
            # and if not specified, it defaults to kilometers.
            distance_val = turf.point_to_line_distance(target_point, line)  # No options passed, defaults to km

            current_result = {
                'zoneName': zone_key,
                'distance': distance_val,
            }

            if result is None or current_result['distance'] < result['distance']:
                result = current_result

    return result

_global_geo_feature_promise_cache = None


async def load_geometry_features_cached():
    """
    Mimics the JS loadGeometryFeatures that caches the promise/future itself.
    """
    global _global_geo_feature_promise_cache

    if _global_geo_feature_promise_cache is None:
        # NOTE we do this async as the file is huge and will block the main thread
        # for several seconds.
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        _global_geo_feature_promise_cache = future

        try:
            def read_and_parse_for_promise():
                try:
                    with open(GEO_GENERATED_FILE_PATH, 'r', encoding='utf-8') as f_sync:
                        data_str_sync = f_sync.read()
                    parsed_sync = json.loads(data_str_sync)
                    loop.call_soon_threadsafe(future.set_result, parsed_sync)
                except Exception as e_sync:
                    print(
                        f"Error loading geo features, please run data generation: {e_sync}"
                    )
                    loop.call_soon_threadsafe(future.set_exception, e_sync)

            loop.run_in_executor(None, read_and_parse_for_promise)

        except Exception as e:  # Should ideally not be hit if executor handles its own exceptions
            print(f"Outer error setting up geo feature load: {e}")
            if not future.done():
                future.set_exception(e)

    return await _global_geo_feature_promise_cache


async def reverse_geocode(lon, lat):
    """
    Performs reverse geocoding for the given longitude and latitude.

    Args:
        lon (float): Longitude of the point.
        lat (float): Latitude of the point.

    Returns:
        str: The name of the zone, or None if no zone is found.
    """
    loaded_data = await load_geometry_features_cached()
    convexhulls = loaded_data.get('convexhulls', [])
    zone_to_geometry_features = loaded_data.get('zoneToGeometryFeatures', {})
    zone_to_lines = loaded_data.get('zoneToLines', {})

    target_point = turf.point([lon, lat])

    convex_hits = []
    if convexhulls:
        for i, feature_item in enumerate(convexhulls):  # Added enumerate for index
            if not isinstance(feature_item, dict):  # Pre-emptive check
                print(
                    f"ERROR utils.py: feature_item at index {i} is NOT a dictionary. Skipping this item. PLEASE INSPECT geo.generated.json.")
                continue

                # Ensure feature_item has the expected GeoJSON Feature structure before passing
            if not (feature_item.get('type') == 'Feature' and isinstance(feature_item.get('geometry'), dict) and
                    feature_item['geometry'].get('type') == 'Polygon' and 'coordinates' in feature_item['geometry']):
                print(
                    f"ERROR utils.py: feature_item at index {i} is not a valid GeoJSON Polygon Feature. Value: '{str(feature_item)[:200]}'. Skipping.")
                continue

            if turf.boolean_point_in_polygon(target_point, feature_item):
                convex_hits.append(feature_item)


                # Step 2: Map to zoneName
    convex_zone_names = [
        feature['properties']['zoneName'] for feature in convex_hits
        if 'properties' in feature and 'zoneName' in feature['properties']
    ]

    # Step 3: Second pass filtering using the polygons themselves
    # JS: .filter((zoneKey) => zoneToGeometryFeatures[zoneKey].some(...))
    precise_hit_zone_names = []
    for zone_key in convex_zone_names:
        geometries_for_zone = zone_to_geometry_features.get(zone_key, [])
        if any(turf.boolean_point_in_polygon(target_point, poly) for poly in geometries_for_zone):
            precise_hit_zone_names.append(zone_key)

    initial_filtering_zones = set(precise_hit_zone_names)

    hit = None
    # JS: let hit = initialFilteringZones.size == 1 ? [...initialFilteringZones][0] : undefined;
    if len(initial_filtering_zones) == 1:
        hit = list(initial_filtering_zones)[0]
    # Python 'None' is equivalent to JS 'undefined' in many contexts for "no value"

    # In case we didn't get one exact hit from the convex hulls and geometries, we find the nearest zone.
    if hit is None:  # JS: if (!hit)
        potential_zones_for_nearest = []
        # JS: initialFilteringZones.size == 0 ? Object.keys(zoneToLines) : [...initialFilteringZones];
        if len(initial_filtering_zones) == 0:
            potential_zones_for_nearest = list(zone_to_lines.keys())
        else:
            potential_zones_for_nearest = list(initial_filtering_zones)

        # Ensure target_point is in a format get_nearest_zone expects
        # (turf.point usually creates a GeoJSON feature, which should be fine)
        nearest_result = get_nearest_zone(potential_zones_for_nearest, zone_to_lines, target_point)

        if nearest_result and nearest_result['distance'] < MAX_NEAREST_ZONE_DISTANCE_KM:
            hit = nearest_result['zoneName']

    # end_time = int(time.time() * 1000)
    # print(f"reverseGeocode for ({lon},{lat}) took {end_time - start_time}ms. Result: {hit}")
    return hit

async def main_example():
    print("Attempting to load geometry features...")
    try:
        features = await load_geometry_features_cached()
        print(f"Successfully loaded features. Number of convex hulls: {len(features.get('convexhulls', []))}")

        lon, lat = -123.2620, 49.2606

        print(f"\nAttempting reverse geocode for lon: {lon}, lat: {lat}")
        zone = await reverse_geocode(lon, lat)
        if zone:
            print(f"Found zone: {zone}")
        else:
            print("No zone found.")

    except Exception as e:
        print(f"An error occurred in the example: {e}")


if __name__ == '__main__':
    if not os.path.exists(GEO_GENERATED_FILE_PATH):
        print(f"Error: {GEO_GENERATED_FILE_PATH} not found.")
        print("Please create a dummy geo.generated.json for testing, e.g.:")
        print("""
        {
            "convexhulls": [],
            "zoneToGeometryFeatures": {},
            "zoneToLines": {}
        }
        """)
    else:
        asyncio.run(main_example())
