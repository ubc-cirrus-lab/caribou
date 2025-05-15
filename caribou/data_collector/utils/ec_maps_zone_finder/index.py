from pathlib import Path

from caribou.data_collector.utils.ec_maps_zone_finder.utils import reverse_geocode
import asyncio

async def main():
    _THIS_FILE_DIR = Path(__file__).resolve().parent
    _FINDER_DATA_CSV_PATH = _THIS_FILE_DIR / "data.csv"
    with open(_FINDER_DATA_CSV_PATH, 'r') as file:
        lines = file.read().strip().split('\n')
        header = lines[0]
        rows = lines[1:]

    results = [header]
    for row in rows:
        if len(row.split(',')) == 2:
            lon, lat = row.split(',')
        elif len(row.split(',')) == 3:
            lon, lat, zone = row.split(',')
        else:
            raise ValueError(f"Invalid row: {row}")
        zone = await reverse_geocode(float(lon), float(lat))
        results.append(f"{lon},{lat},{zone}")

    with open(_FINDER_DATA_CSV_PATH, 'w') as file:
        file.write('\n'.join(results) + '\n')

    # print(f"Processed {len(rows)} coordinates")

if __name__ == "__main__":
    asyncio.run(main())
