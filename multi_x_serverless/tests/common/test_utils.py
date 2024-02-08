import unittest
from typing import Callable, Any
import inspect
import importlib
import textwrap
import ast

from multi_x_serverless.common.utils import get_function_source


class TestGetFunctionSource(unittest.TestCase):
    def test_get_function_source(self):
        def test_function():
            print("Hello, world!")

        source_code = get_function_source(test_function)

        source_code = "".join(source_code.split())

        self.assertIn('print("Hello,world!")', source_code)

    def test_get_function_source_with_called_function(self):
        def called_function():
            print("Hello from called function!")

        def test_function():
            print("Hello, world!")
            called_function()

        source_code = get_function_source(test_function)

        source_code = "".join(source_code.split())

        self.assertIn('print("Hello,world!")', source_code)


if __name__ == "__main__":
    unittest.main()
