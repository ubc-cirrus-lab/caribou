import pytest

from caribou.deployment_solver.models.instance_indexer import InstanceIndexer
from caribou.deployment_solver.models.region_indexer import RegionIndexer

N_REGIONS = 10
N_INSTANCES = 10


@pytest.fixture
def simple_region_indexer():
    return RegionIndexer(
        [f"region_{i}" for i in range(N_REGIONS)],
    )


@pytest.fixture
def simple_instance_indexer():
    return InstanceIndexer(
        [{"instance_name": f"instance_{i}"} for i in range(N_INSTANCES)],
    )


@pytest.fixture
def simple_workflow_config():
    pass
