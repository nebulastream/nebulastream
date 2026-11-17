"""NebulaStream entry points for the UDFBench scalar UDFs (CREATE FUNCTION ... ENTRYPOINT 'udfbench_scalar.<name>').

scalar/<name>.py are the UDFBench DuckDB files, unchanged. UDFBench attaches them to a class Scalar and registers the
methods of a Scalar() instance (engines/duckdb/udfs/scalar/__init__.py, queries/exec.py); this module does the same.
UDFBench's __init__.py is not copied because it imports pyarrow and pandas, which no UDF uses.
"""
import importlib.util
import os


class Scalar:
    def __init__(self):
        pass


_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scalar")
for _file in sorted(os.listdir(_DIR)):
    if _file.endswith(".py"):
        _name = _file[:-3]
        _spec = importlib.util.spec_from_file_location(_name, os.path.join(_DIR, _file))
        _module = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_module)
        setattr(Scalar, _name, getattr(_module, _name))

_scalar = Scalar()
for _name in [n for n in vars(Scalar) if not n.startswith("_")]:
    globals()[_name] = getattr(_scalar, _name)
