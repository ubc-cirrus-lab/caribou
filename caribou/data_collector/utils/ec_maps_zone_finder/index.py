import asyncio
from pathlib import Path

from caribou.data_collector.utils.ec_maps_zone_finder.utils import reverse_geocode


async def find_zone() -> None:
    _this_file_dir = Path(__file__).resolve().parent
    _finder_data_csv_path = _this_file_dir / "data.csv"
    with open(_finder_data_csv_path, "r", encoding="utf-8") as file:
        lines = file.read().strip().split("\n")
        header = lines[0]
        rows = lines[1:]

    results = [header]
    for row in rows:
        if len(row.split(",")) == 2:
            lon, lat = row.split(",")
        elif len(row.split(",")) == 3:
            lon, lat, _ = row.split(",")
        else:
            raise ValueError(f"Invalid row: {row}")
        zone: str | None = await reverse_geocode(float(lon), float(lat))
        if zone is None:
            zone = ""
        results.append(f"{lon},{lat},{zone}")

    with open(_finder_data_csv_path, "w", encoding="utf-8") as file:
        file.write("\n".join(results) + "\n")


if __name__ == "__main__":
    asyncio.run(find_zone())
