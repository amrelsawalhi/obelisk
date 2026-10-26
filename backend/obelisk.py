"""
Top-level obelisk package shim enabling `import obelisk` across the repository and notebooks.
"""
import sys
from pathlib import Path

# Ensure backend root is in sys.path
backend_dir = Path(__file__).resolve().parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from app.sdk.obelisk import (
    table,
    query,
    query_arrow,
    save,
    materialize_sql,
    spark,
    TableWrapper,
    var,
)

__all__ = [
    "table",
    "query",
    "query_arrow",
    "save",
    "materialize_sql",
    "spark",
    "TableWrapper",
    "var",
]
