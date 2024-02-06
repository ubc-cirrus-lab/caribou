import ast
import inspect
from typing import Callable, Any

def str_to_bool(s: str) -> bool:
    return s.lower() in ["true", "1", "t", "y", "yes"]


def get_function_source(function_callable: Callable[..., Any]) -> str:
    source_code = ""

    try:
        # Get the source of the initial function
        source = inspect.getsource(function_callable)
        source_code += source + "\n\n# Called functions:\n\n"
        
        # Parse the source code of the function
        tree = ast.parse(source)

        # Dictionary to hold the functions we've already included
        included_functions = {}

        # Function to process each node in the AST
        def process_node(node):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                # Assuming the function is defined in the same file or imported directly
                name = node.func.id
                if name not in included_functions:  # Check if function was not already included
                    try:
                        # Retrieve the function object by name
                        f = eval(name)
                        # Add the source code of the called function
                        func_source = inspect.getsource(f)
                        included_functions[name] = True
                        nonlocal source_code
                        source_code += f"\n# Source of {name}:\n{func_source}\n"
                    except Exception as e:
                        pass

            # Process all child nodes
            for child in ast.iter_child_nodes(node):
                process_node(child)
        
        # Process each node in the AST of the source code
        for node in ast.walk(tree):
            process_node(node)

    except Exception as e:
        return f"Error: {e}"

    return source_code