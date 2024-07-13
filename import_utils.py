import os
from dagster import AssetsDefinition
from typing import Any, List, Dict
from collections.abc import Sequence
import importlib

def _get_dagster_name(obj: Any) -> str:
    return obj.op.name if type(obj) == AssetsDefinition else obj.name


def _get_instances_in_file(file: str, dtypes: List[type]) -> Dict[str, Any]:
    """
    Find instances of dtypes in module scope for given .py file'.
    """
    file_name, ext = os.path.splitext(file)
    if ext == ".py":
        module_name = file_name.replace("/", ".")
        module = importlib.import_module(module_name)

        # add dagster objects in module scope
        instances = {
            _get_dagster_name(obj): obj
            for name, obj in module.__dict__.items()
            if not name.startswith("__") 
            and any([isinstance(obj, dtype) for dtype in dtypes])
        }
        # add dagster objects inside sequences in module scope
        instances |= {
            _get_dagster_name(obj): obj
            for name, item in module.__dict__.items()
            if isinstance(item, Sequence) and not name.startswith('__')
            for obj in item
            if any([isinstance(obj, dtype) for dtype in dtypes])
        }
        return instances


def get_all_instances(root: str, dtypes: List[type]) -> List:
    """
    Find all instances of dtypes in root directory or file using DFS.
    """
    all_instances = {}

    def DFS(path: str):
        if os.path.isfile(path):
            instances = _get_instances_in_file(path, dtypes)
            if instances:
                nonlocal all_instances
                all_instances |= instances
            return
        else:
            for member in os.listdir(path):
                if not member.startswith("__"):
                    resolved_path = os.path.join(path, member)
                    DFS(resolved_path)

    DFS(root)
    return list(all_instances.values())
