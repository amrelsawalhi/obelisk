"""
Notebook obelisk package shim enabling `import obelisk` inside git_repo/notebooks/.
"""
import sys
from pathlib import Path

# Add backend directory to sys.path
backend_dir = Path(__file__).resolve().parent.parent.parent / "backend"
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from app.sdk.obelisk import (
    table,
    query,
    query_arrow,
    save,
    materialize_sql,
    var,
    spark,
    TableWrapper,
)

__all__ = [
    "table",
    "query",
    "query_arrow",
    "save",
    "materialize_sql",
    "var",
    "spark",
    "TableWrapper",
]
