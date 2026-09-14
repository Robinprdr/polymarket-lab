"""Regression guard for the intentionally closed network and dependency surface."""
import ast
from pathlib import Path


def test_network_surface_and_dependencies_are_read_only():
    root = Path(__file__).parents[1]
    package = root / "polymarket_lab"
    forbidden_imports = {"web3", "eth_account", "py_clob_client", "polymarket"}
    forbidden_methods = {"post", "put", "patch", "delete", "request", "place_order", "create_order", "cancel_order"}
    for path in package.glob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert not any(x.name.split(".")[0] in forbidden_imports for x in node.names)
            if isinstance(node, ast.ImportFrom):
                assert (node.module or "").split(".")[0] not in forbidden_imports
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in forbidden_methods
    for filename in ("complement.py", "opportunities.py"):
        tree = ast.parse((package / filename).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert not any(x.name.split(".")[0] in {"httpx", "websockets", "socket", "urllib"} for x in node.names)
            if isinstance(node, ast.ImportFrom):
                assert (node.module or "").split(".")[0] not in {"httpx", "websockets", "socket", "urllib", "public_api"}
    assert (root / "requirements.txt").read_text().splitlines() == ["httpx==0.28.1", "websockets==15.0.1"]
