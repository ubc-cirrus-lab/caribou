import multiprocessing
from caribou.monitors.deployment_manager import DeploymentManager
from memory_profiler import profile

if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')
    deployment_manager = DeploymentManager()
    deployment_manager.check()
