"""
DAG Loader Engine:
Auto-discovers, imports, and manages custom Dagster assets, jobs, and schedules
defined in `git_repo/dags/*.py`.
Provides unified introspection for both Dagster Definitions and the Obelisk Pipelines UI.
"""

import os
import sys
import ast
import inspect
import importlib.util
from pathlib import Path
from typing import List, Dict, Any, Optional

try:
    import dagster as dg
except ImportError:
    dg = None


class DagLoader:
    def __init__(self, dags_dir: Optional[Path] = None):
        if dags_dir is None:
            # Resolve workspace root git_repo/dags
            root = Path(__file__).resolve().parent.parent.parent.parent
            self.dags_dir = (root / "git_repo" / "dags").resolve()
        else:
            self.dags_dir = dags_dir.resolve()

        self.dags_dir.mkdir(parents=True, exist_ok=True)

    def list_dag_files(self) -> List[Dict[str, Any]]:
        """Scans git_repo/dags/*.py and returns file information."""
        if not self.dags_dir.exists():
            return []

        results = []
        for file in sorted(self.dags_dir.glob("*.py")):
            if file.name.startswith("_") or file.name.startswith("."):
                continue

            stat = file.stat()
            results.append({
                "filename": file.name,
                "name": file.stem,
                "path": str(file),
                "size_bytes": stat.st_size,
                "modified_at": stat.st_mtime,
            })
        return results

    def get_dag_code(self, dag_name: str) -> Optional[str]:
        """Returns the raw Python code of a DAG file."""
        file_path = self.dags_dir / f"{dag_name}.py"
        if not file_path.exists():
            # Try matching filename directly
            file_path = self.dags_dir / dag_name
            if not file_path.exists():
                return None
        return file_path.read_text(encoding="utf-8")

    def save_dag_code(self, dag_name: str, code: str) -> str:
        """Saves or updates code for a DAG file."""
        clean_name = dag_name.replace(".py", "").strip()
        file_path = self.dags_dir / f"{clean_name}.py"
        file_path.write_text(code, encoding="utf-8")
        return str(file_path)

    def delete_dag_file(self, dag_name: str) -> bool:
        """Deletes a DAG file."""
        clean_name = dag_name.replace(".py", "").strip()
        file_path = self.dags_dir / f"{clean_name}.py"
        if file_path.exists():
            file_path.unlink()
            return True
        return False

    def load_dagster_assets(self) -> List[Any]:
        """
        Dynamically imports all Python modules in git_repo/dags/
        and collects all Dagster AssetsDefinition objects.
        """
        if dg is None:
            return []

        assets = []
        for file_info in self.list_dag_files():
            file_path = Path(file_info["path"])
            module_name = f"obelisk_dags_{file_path.stem}"

            try:
                spec = importlib.util.spec_from_file_location(module_name, file_path)
                if spec is None or spec.loader is None:
                    continue

                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)

                # Find all Dagster AssetsDefinition objects
                for attr_name in dir(module):
                    attr_val = getattr(module, attr_name)
                    if isinstance(attr_val, dg.AssetsDefinition):
                        assets.append(attr_val)
            except Exception as e:
                print(f"[DagLoader] Warning: Failed to import DAG module {file_path.name}: {e}")

        return assets

    def introspect_dag_nodes(self) -> List[Dict[str, Any]]:
        """
        Inspects all DAG files and extracts asset metadata (name, deps, group, description, code)
        without strictly requiring a running Dagster daemon.
        First parses AST from the Python source code for 100% accurate, instantaneous reflection of
        any user edits. Falls back to dynamic module inspection if needed.
        """
        nodes = []
        for file_info in self.list_dag_files():
            code = self.get_dag_code(file_info["name"]) or ""
            file_path = Path(file_info["path"])

            # 1. Primary: Fast, exact AST parsing of @asset decorator
            ast_data = self._parse_ast(code, file_info["name"])
            if ast_data:
                nodes.append({
                    "id": f"dag.{ast_data['name']}",
                    "name": ast_data["name"],
                    "filename": file_info["filename"],
                    "layer": "custom",
                    "group": ast_data["group"],
                    "deps": ast_data["deps"],
                    "description": ast_data["description"] or f"Custom pipeline asset from {file_info['filename']}",
                    "sql_code": code,
                })
                continue

            # 2. Secondary: Dynamic import inspection
            module_name = f"obelisk_inspect_{file_path.stem}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, file_path)
                if spec and spec.loader:
                    mod = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(mod)

                    for attr in dir(mod):
                        val = getattr(mod, attr)
                        if dg and isinstance(val, dg.AssetsDefinition):
                            asset_key = next(iter(val.keys)).to_user_string()
                            raw_deps = val.asset_deps.get(next(iter(val.keys)), set())
                            asset_deps = []
                            for dep in raw_deps:
                                if hasattr(dep, "to_user_string"):
                                    asset_deps.append(dep.to_user_string())
                                elif hasattr(dep, "asset_key"):
                                    asset_deps.append(dep.asset_key.to_user_string())
                                else:
                                    asset_deps.append(str(dep))

                            nodes.append({
                                "id": f"dag.{asset_key}",
                                "name": asset_key,
                                "filename": file_info["filename"],
                                "layer": "custom",
                                "group": val.group_names_by_key.get(next(iter(val.keys)), "custom_dags"),
                                "deps": asset_deps,
                                "description": val.description or "",
                                "sql_code": code,
                            })
                            break
                    else:
                        nodes.append({
                            "id": f"dag.{file_info['name']}",
                            "name": file_info["name"],
                            "filename": file_info["filename"],
                            "layer": "custom",
                            "group": "custom_dags",
                            "deps": [],
                            "description": f"Custom pipeline asset from {file_info['filename']}",
                            "sql_code": code,
                        })
            except Exception:
                nodes.append({
                    "id": f"dag.{file_info['name']}",
                    "name": file_info["name"],
                    "filename": file_info["filename"],
                    "layer": "custom",
                    "group": "custom_dags",
                    "deps": [],
                    "description": f"Custom pipeline asset from {file_info['filename']}",
                    "sql_code": code,
                })

        return nodes

    def _parse_ast(self, code: str, default_name: str) -> Optional[Dict[str, Any]]:
        """Parses @asset(...) decorator metadata from Python source code AST."""
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    for dec in node.decorator_list:
                        is_asset = False
                        keywords = []
                        if isinstance(dec, ast.Call):
                            func_name = ""
                            if isinstance(dec.func, ast.Name):
                                func_name = dec.func.id
                            elif isinstance(dec.func, ast.Attribute):
                                func_name = dec.func.attr
                            if func_name == "asset":
                                is_asset = True
                                keywords = dec.keywords
                        elif isinstance(dec, ast.Name) and dec.id == "asset":
                            is_asset = True
                        elif isinstance(dec, ast.Attribute) and dec.attr == "asset":
                            is_asset = True

                        if is_asset:
                            asset_name = default_name
                            deps = []
                            group = "custom_dags"
                            desc = ""
                            for kw in keywords:
                                if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                                    asset_name = str(kw.value.value)
                                elif kw.arg == "deps":
                                    if isinstance(kw.value, (ast.List, ast.Tuple, ast.Set)):
                                        for elt in kw.value.elts:
                                            if isinstance(elt, ast.Constant):
                                                deps.append(str(elt.value))
                                elif kw.arg == "group_name" and isinstance(kw.value, ast.Constant):
                                    group = str(kw.value.value)
                                elif kw.arg == "description" and isinstance(kw.value, ast.Constant):
                                    desc = str(kw.value.value)
                            return {
                                "name": asset_name,
                                "deps": deps,
                                "group": group,
                                "description": desc,
                            }
        except Exception:
            return None
        return None

    def get_dag_graph_nodes(self) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Returns introspected custom DAG nodes and upstream edges."""
        nodes = self.introspect_dag_nodes()
        edges = []
        for n in nodes:
            for dep in n.get("deps", []):
                edges.append({
                    "id": f"{dep}->{n['id']}",
                    "source": dep,
                    "target": n["id"],
                })
        return nodes, edges

    # Aliases for convenience
    list_dags = list_dag_files
    load_dag_assets = load_dagster_assets


# Global singleton instance
dag_loader = DagLoader()
