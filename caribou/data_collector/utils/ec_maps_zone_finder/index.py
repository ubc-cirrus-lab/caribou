from caribou.data_collector.utils.ec_maps_zone_finder.utils import reverse_geocode


async def find_zone(latitude: float, longitude: float) -> str | None:
    zone: str | None = await reverse_geocode(float(longitude), float(latitude))
    return zone
