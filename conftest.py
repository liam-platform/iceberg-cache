"""
Root conftest.py — ensures src/ is on sys.path so that bare imports such as
``from core.cache_node import ArrowCacheNode`` work in all test runners and
editors, even when the project is not installed as a package.

The canonical path-injection is done via ``tool.pytest.ini_options.pythonpath``
in pyproject.toml; this file is a belt-and-suspenders fallback.
"""
import sys
import pathlib

_SRC = pathlib.Path(__file__).parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
