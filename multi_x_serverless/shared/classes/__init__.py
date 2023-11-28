import os

from .datacenter import Datacenter
from .location import Location, get_location

files = os.listdir(os.path.dirname(__file__))
files.remove("__init__.py")
__all__ = [f[:-3] for f in files if f.endswith(".py")]
