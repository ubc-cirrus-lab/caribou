from multi_x_serverless.shared.classes.location import Location


class Datacenter:
    def __init__(self, name: str, region: str, provider: str, location: Location, tier: int = 1):
        self.name = name
        self.region = region
        self.provider = provider
        self.location = location
        self.tier = tier  # Tier is only used for GCP

    def get_name(self) -> str:
        return self.name

    def get_region(self) -> str:
        return self.region

    def get_location(self) -> str:
        return self.location

    def __str__(self) -> str:
        return self.region

    def __repr__(self) -> str:
        return self.region
