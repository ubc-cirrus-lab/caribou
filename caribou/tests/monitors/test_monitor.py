import unittest
from unittest.mock import patch, MagicMock
from caribou.common.models.endpoints import Endpoints
from caribou.monitors.monitor import Monitor


class ConcreteMonitor(Monitor):
    def check(self) -> None:
        pass


class TestMonitor(unittest.TestCase):
    def test_check_not_implemented(self):
        with self.assertRaises(TypeError):
            Monitor()

    def test_check_implemented(self):
        monitor = ConcreteMonitor()
        try:
            monitor.check()
        except NotImplementedError:
            self.fail("check() raised NotImplementedError unexpectedly!")


if __name__ == "__main__":
    unittest.main()
