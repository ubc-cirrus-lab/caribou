import math
from typing import Any

# Constants
EARTH_RADIUS = 6371008.8

FACTORS = {
    "kilometers": EARTH_RADIUS / 1000,
    "metres": EARTH_RADIUS,
    "degrees": EARTH_RADIUS / 111325,
}

CoordInput = list[float] | dict[str, Any] | None
PointCoordinates = list[float]


# Helper Functions
def degrees_to_radians(degrees: float) -> float:
    radians = degrees % 360
    return (radians * math.pi) / 180


def radians_to_length(radians: float, units: str = "kilometers") -> float:
    factor = FACTORS.get(units)
    if factor is None:
        raise ValueError(units + " units is invalid")
    return radians * factor


def get_coord(coord: CoordInput) -> PointCoordinates:
    if coord is None:
        raise ValueError("coord is required")

    if not isinstance(coord, list):
        if (
            isinstance(coord, dict)
            and coord.get("type") == "Feature"
            and coord.get("geometry") is not None
            and isinstance(coord.get("geometry"), dict)
            and coord["geometry"].get("type") == "Point"
        ):
            return coord["geometry"]["coordinates"]
        if isinstance(coord, dict) and coord.get("type") == "Point":
            return coord["coordinates"]

    if (
        isinstance(coord, list)
        and len(coord) >= 2
        and not isinstance(coord[0], list)
        and not isinstance(coord[1], list)
    ):
        if all(isinstance(c, (int, float)) for c in coord[:2]):
            return coord

    raise ValueError("coord must be GeoJSON Point or an Array of numbers")


def feature(geom: dict[str, Any], properties: dict[str, Any] | None = None) -> dict[str, Any]:
    if properties is None:
        properties = {}
    return {
        "type": "Feature",
        "properties": properties,
        "geometry": geom,
    }


# Main Functions
def point(coordinates: list[float], properties: dict[str, Any] | None = None) -> dict[str, Any]:
    if properties is None:
        properties = {}
    if coordinates is None:
        raise ValueError("coordinates is required")
    if not isinstance(coordinates, list):
        raise ValueError("coordinates must be a list")

    return feature(
        {
            "type": "Point",
            "coordinates": coordinates,
        },
        properties,
    )


def distance(from_pt: Any, to_pt: Any, options: dict[str, Any] | None = None) -> float:
    if options is None:
        options = {}

    coordinates1 = get_coord(from_pt)
    coordinates2 = get_coord(to_pt)

    d_lat = degrees_to_radians(coordinates2[1] - coordinates1[1])
    d_lon = degrees_to_radians(coordinates2[0] - coordinates1[0])
    lat1 = degrees_to_radians(coordinates1[1])
    lat2 = degrees_to_radians(coordinates2[1])

    a = math.pow(math.sin(d_lat / 2), 2) + math.pow(math.sin(d_lon / 2), 2) * math.cos(lat1) * math.cos(lat2)

    return radians_to_length(2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


def dot(u: list[float], v: list[float]) -> float:
    return u[0] * v[0] + u[1] * v[1]


def distance_to_segment(
    p: list[float], a: list[float], b: list[float], options_for_distance_call: dict[str, Any] | None = None
) -> float:
    if options_for_distance_call is None:
        options_for_distance_call = {}

    v = [b[0] - a[0], b[1] - a[1]]
    w = [p[0] - a[0], p[1] - a[1]]

    c1 = dot(w, v)

    if c1 <= 0:
        return distance(p, a, options_for_distance_call)

    c2 = dot(v, v)
    if c2 <= c1:
        return distance(p, b, options_for_distance_call)

    b2 = c1 / c2
    pb = [a[0] + b2 * v[0], a[1] + b2 * v[1]]
    return distance(p, pb, options_for_distance_call)


def point_to_line_distance(pt_input: Any, line_input: Any, options: dict[str, Any] | None = None) -> float:
    if options is None:
        options = {}

    _method = options.get("method", "geodesic")
    units_for_final_result_if_conversion_was_done = options.get("units", "kilometers")

    _pt_feature_like = _get_pt_feature(pt_input)

    _line_feature_like = _get_line_feature(line_input)

    if "geometry" in _pt_feature_like and isinstance(_pt_feature_like["geometry"], dict):
        p_coords = _pt_feature_like["geometry"]["coordinates"]
    elif _pt_feature_like.get("type") == "Point" and "coordinates" in _pt_feature_like:
        p_coords = _pt_feature_like["coordinates"]
    else:
        raise ValueError("Cannot extract coordinates from pt_input")

    if "geometry" in _line_feature_like and isinstance(_line_feature_like["geometry"], dict):
        line_coords_list = _line_feature_like["geometry"]["coordinates"]
    elif _line_feature_like.get("type") == "LineString" and "coordinates" in _line_feature_like:
        line_coords_list = _line_feature_like["coordinates"]
    else:
        raise ValueError("Cannot extract coordinates from line_input")

    min_dist_val = float("inf")

    if not line_coords_list or len(line_coords_list) < 1:
        raise ValueError("LineString must have at least one point.")

    if len(line_coords_list) == 1:
        return distance(p_coords, line_coords_list[0], {"units": units_for_final_result_if_conversion_was_done})

    distance_options_for_segment: dict[str, Any] = {}

    for i in range(len(line_coords_list) - 1):
        a = line_coords_list[i]
        b = line_coords_list[i + 1]
        d = distance_to_segment(p_coords, a, b, distance_options_for_segment)
        min_dist_val = min(min_dist_val, d)

    if min_dist_val != float("inf") and units_for_final_result_if_conversion_was_done != "kilometers":
        radians_val = min_dist_val / FACTORS["kilometers"]
        return radians_to_length(radians_val, units_for_final_result_if_conversion_was_done)

    return min_dist_val


def _get_pt_feature(pt_input: Any) -> dict[str, Any]:
    if isinstance(pt_input, list):
        return feature({"type": "Point", "coordinates": pt_input})

    if isinstance(pt_input, dict):
        return pt_input

    raise TypeError("pt_input must be a list or a dict")


def _get_line_feature(line_input: Any) -> dict[str, Any]:
    if isinstance(line_input, list):
        return feature({"type": "LineString", "coordinates": line_input})

    if isinstance(line_input, dict):
        return line_input

    raise TypeError("line_input must be a list of coordinates or a dict")


def in_ring(pt_coords: list[float], ring_coords: list[list[float]]) -> bool:
    inside = False
    x = pt_coords[0]
    y = pt_coords[1]

    num_vertices = len(ring_coords)
    if num_vertices == 0:
        return False

    j = num_vertices - 1  # Previous vertex index
    for i in range(num_vertices):
        xi, yi = ring_coords[i]
        xj, yj = ring_coords[j]

        intersect = False
        if (yi > y) != (yj > y):
            if yj != yi:
                x_intersection = ((xj - xi) * (y - yi)) / (yj - yi) + xi
                if x < x_intersection:
                    intersect = True
        if intersect:
            inside = not inside
        j = i

    return inside


def boolean_point_in_polygon(point_input_bip: Any, polygon_input_bip: dict[str, Any]) -> bool:
    pt = get_coord(point_input_bip)

    polys_rings = None

    if not isinstance(polygon_input_bip, dict):
        raise TypeError("Polygon input must be a dictionary (GeoJSON Polygon Feature or Polygon Geometry).")

    input_type = polygon_input_bip.get("type")

    if input_type == "Feature":
        geometry = polygon_input_bip.get("geometry")
        if not isinstance(geometry, dict):
            raise ValueError("Input GeoJSON Feature's 'geometry' must be a dictionary.")

        geometry_type = geometry.get("type")
        if geometry_type == "Polygon":
            polys_rings = geometry.get("coordinates")
        else:
            raise ValueError(f"Input Feature's geometry type must be 'Polygon'. Got: {geometry_type}")
    elif input_type == "Polygon":
        polys_rings = polygon_input_bip.get("coordinates")
    else:
        raise ValueError(f"Input must be a GeoJSON Feature or a Polygon geometry. Type: {input_type}")

    if polys_rings is None or not isinstance(polys_rings, list):
        raise ValueError("Polygon 'coordinates' must be a list of rings (e.g., [[[lon, lat],...]]).")

    inside = False
    for ring_coords in polys_rings:
        if not isinstance(ring_coords, list):
            raise ValueError("Each ring in polygon coordinates must be a list of coordinate pairs.")
        if not ring_coords:
            continue
        if in_ring(pt, ring_coords):
            inside = not inside

    return inside
