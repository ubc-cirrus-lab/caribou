import ast
import inspect
import textwrap
from typing import Any, Callable


def str_to_bool(s: str) -> bool:
    return s.lower() in ["true", "1", "t", "y", "yes"]


def get_function_source(function_callable: Callable[..., Any]) -> str:
    source_code = ""

    # Get the source of the initial function
    source = inspect.getsource(function_callable)
    source = textwrap.dedent(source)  # Remove leading whitespace
    source_code += source + "\n\n# Called functions:\n\n"

    # Parse the source code of the function
    tree = ast.parse(source)

    # Dictionary to hold the functions we've already included
    included_functions = {}

    # Function to process each node in the AST
    def process_node(node: ast.AST) -> None:
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            # Assuming the function is defined in the same file or imported directly
            name = node.func.id
            if name not in included_functions:  # Check if function was not already included
                try:
                    # Retrieve the function object by name
                    # WARNING: This is not safe but since this is only ever executed
                    # in a local environment, it is safe to use eval here.
                    f = eval(name)  # pylint: disable=eval-used
                    # Add the source code of the called function
                    func_source = inspect.getsource(f)
                    included_functions[name] = True
                    nonlocal source_code
                    source_code += f"\n# Source of {name}:\n{func_source}\n"
                except Exception:  # pylint: disable=broad-except
                    pass

        # Process all child nodes
        for child in ast.iter_child_nodes(node):
            process_node(child)

    # Process each node in the AST of the source code
    for node in ast.walk(tree):
        process_node(node)

    return source_code
