import inspect
import ast
import textwrap
from typing import Any, List, Callable


def _remove_duplicates(lst: List[Any]) -> List[Any]:
    unique_list = []
    for item in lst:
        if item not in unique_list:
            unique_list.append(item)
    return unique_list


def _get_called_function_objects(func: Callable) -> List[Callable]:
    """Retrieves a list of callable function objects invoked within the given function's body."""
    try:
        # Retrieve the source code of the function
        source_code = inspect.getsource(func)
    except Exception as e:
        raise RuntimeError(f"Unable to retrieve source code for function {func.__name__}: {e}")
    
    # Normalize indentation
    source_code = textwrap.dedent(source_code)
    
    try:
        # Parse the source code into an Abstract Syntax Tree (AST)
        tree = ast.parse(source_code)
    except SyntaxError as e:
        raise RuntimeError(f"Syntax error while parsing source code of {func.__name__}: {e}")

    class FunctionCallVisitor(ast.NodeVisitor):
        """AST Visitor to collect function calls."""
        def __init__(self):
            self.called_functions = []

        def visit_Call(self, node: ast.Call):
            """Visit a call node and collect function names."""
            if isinstance(node.func, ast.Name):  # Simple function call: `foo()`
                self.called_functions.append(node.func.id)
            elif isinstance(node.func, ast.Attribute):  # Method call: `obj.method()`
                self.called_functions.append(node.func.attr)
            self.generic_visit(node)

    # Traverse the AST to collect function calls
    visitor = FunctionCallVisitor()
    visitor.visit(tree)

    # Resolve function names to actual callable objects
    resolved_functions = []
    func_globals = func.__globals__

    for func_name in visitor.called_functions:
        # Ensure the function name is in the global scope and is callable
        if func_name in func_globals and callable(func_globals[func_name]):
            resolved_functions.append(func_globals[func_name])

    return resolved_functions


def _get_called_function_invocations(func: Callable) -> List[dict]:
    """Retrieves details of function calls made within the given function's body."""
    try:
        # Retrieve and normalize the source code
        source_code = inspect.getsource(func)
        source_code = textwrap.dedent(source_code)
    except Exception as e:
        raise RuntimeError(f"Unable to retrieve source code for function {func.__name__}: {e}")

    try:
        # Parse the source code into an Abstract Syntax Tree (AST)
        tree = ast.parse(source_code)
    except SyntaxError as e:
        raise RuntimeError(f"Syntax error while parsing source code of {func.__name__}: {e}")

    class FunctionInvocationVisitor(ast.NodeVisitor):
        """AST Visitor to collect details of function calls."""
        def __init__(self):
            self.invocations = []

        def visit_Call(self, node: ast.Call):
            """Capture function call details."""
            invocation = {
                "function_name": None,
                "arguments": [],
                "keywords": {}
            }

            # Function name (handles both `foo()` and `obj.method()`)
            if isinstance(node.func, ast.Name):  # Simple function call
                invocation["function_name"] = node.func.id
            elif isinstance(node.func, ast.Attribute):  # Method call
                invocation["function_name"] = node.func.attr

            # Positional arguments
            invocation["arguments"] = [ast.dump(arg) for arg in node.args]

            # Keyword arguments
            invocation["keywords"] = {
                kw.arg: ast.dump(kw.value) for kw in node.keywords if kw.arg
            }

            self.invocations.append(invocation)
            self.generic_visit(node)

    # Traverse the AST to collect function invocations
    visitor = FunctionInvocationVisitor()
    visitor.visit(tree)

    return visitor.invocations
