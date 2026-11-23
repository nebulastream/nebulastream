# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Splits a HiPy UDF into a linear chain of native and Python stages.

HiPy compiles what it can of a UDF natively and leaves the rest to CPython. This module turns HiPy's IR (the serialized
hipy.ir.Module) into a chain native, python, native, ... that a query plan can run as one operator per stage: native stages
as Nautilus code in the pipeline, Python stages as Python UDFs. Between two stages, every live value is a field of the record.

1. Inline: the calls of the entry function are inlined, so it is one body (loop bodies stay functions of their loop).
2. If-conversion: every if/else of that body becomes straight-line code. Each op gets a guard (a bool value: the conjunction
   of the conditions of the branches it was in), and each result of an if/else becomes a Select of the two yielded values.
3. Classification: an op runs in Python if it is a CPython operation, takes or makes a Python object, or is a loop (or call)
   whose body has such an operation. Constants, function references and record ops are free: a stage that needs them
   recomputes them. Every other op is native.
4. Scheduling: the ops are placed into stages greedily, taking every ready op of the current kind before switching, which
   keeps the number of stages small. Python ops keep their order, as they can have side effects.
5. A Python object cannot be a field. Where one lives across a native stage, the stages from its definition to its last
   use merge into one Python stage (a native op can always run in Python, with the same result).

Each stage lists its inputs and outputs (value ids with HiPy types), its ops, and Python source that computes it: what a Python
stage runs, and a reference for a native stage.
"""

import copy
import json

FREE = {"Constant", "FunctionRef", "Undef", "MakeRecord", "RecordGet", "NoOp", "Alias"}
# Values that live only in Python: CPython objects, and lists and dicts, which the Nautilus backend has no representation for.
PYTHON_TYPES = {"py_object", "list", "dict"}
# The builtins that HiPyPhysicalFunction runs natively (supportedBuiltins there, without the CPython ones). A UDF op with any
# other builtin runs in a Python stage.
NATIVE_BUILTINS = {
    "undef", "error", "range.iter", "while.iter", "scalar.string.iter", "nullable.make", "nullable.null", "nullable.is_null",
    "nullable.get_value", "scalar.bool.and", "scalar.bool.or", "scalar.bool.not", "scalar.int.add", "scalar.int.sub",
    "scalar.int.mul", "scalar.int.div", "scalar.int.mod", "scalar.int.lshift", "scalar.int.and", "scalar.int.xor",
    "scalar.int.compare.eq", "scalar.int.compare.neq", "scalar.int.compare.lt", "scalar.int.compare.lte",
    "scalar.int.compare.gt", "scalar.int.compare.gte", "scalar.int.to_string", "scalar.int.from_string",
    "scalar.int.pyint_to_int64", "scalar.float.add", "scalar.float.sub", "scalar.float.mul", "scalar.float.div",
    "scalar.float.mod", "scalar.float.neg", "scalar.float.pow", "scalar.float.ceil", "scalar.float.exp", "scalar.float.log",
    "scalar.float.sqrt", "scalar.float.sin", "scalar.float.cos", "scalar.float.acos", "scalar.float.atan2",
    "scalar.float.copysign", "scalar.float.round", "scalar.float.compare.eq", "scalar.float.compare.neq",
    "scalar.float.compare.lt", "scalar.float.compare.lte", "scalar.float.compare.gt", "scalar.float.compare.gte",
    "scalar.float.from_int", "scalar.float.to_int", "scalar.float.to_string", "scalar.float.from_string",
    "scalar.string.compare.eq", "scalar.string.compare.lt", "scalar.string.compare.lte", "scalar.string.concatenate",
    "scalar.string.contains", "scalar.string.find", "scalar.string.rfind", "scalar.string.length", "scalar.string.lower",
    "scalar.string.upper", "scalar.string.ord", "scalar.string.replace", "scalar.string.strip", "scalar.string.substr",
    "scalar.string.at",
}
LOOPS = {"range.iter", "while.iter", "scalar.string.iter", "list.iter"}
SCALARS = {"bool", "int", "pyint", "float", "string"}


# ---------------------------------------------------------------------------------------------------------------------------
# IR helpers. Ops are the JSON dicts of ir.Module.serialize(); a value is {"kind": "value", "id": n, "type": {...}}.
# ---------------------------------------------------------------------------------------------------------------------------

def is_value(node):
    return isinstance(node, dict) and node.get("kind") == "value"


def uses(op):
    """The ids of the values that `op` reads directly (not those that nested blocks read)."""
    kind = op["kind"]
    values = []
    if kind in ("CallBuiltin", "Call"):
        values = list(op["args"])
        if is_value(op.get("fnref")):
            values.append(op["fnref"])
    elif kind in ("Yield", "Return"):
        values = list(op["values"])
    elif kind == "IfElse":
        values = [op["cond"]]
    elif kind == "MakeRecord":
        values = list(op["values"].values())
    elif kind == "RecordGet":
        values = [op["record"]]
    elif kind == "FunctionRef":
        values = [op["closure"]] if "closure" in op else []
    elif kind == "PyGetAttr":
        values = [op["on"]]
    elif kind == "PySetAttr":
        values = [op["on"], op["value"]]
    elif kind == "PyCall2":
        values = [op["callable"], *op["args"], *[keyword["value"] for keyword in op["kw_args"]]]
    elif kind in ("Select", "Alias"):
        values = list(op["args"])
    ids = [value["id"] for value in values]
    if op.get("guard") is not None:
        ids.append(op["guard"])
    return ids


def defines(op):
    ids = [op["result"]["id"]] if "result" in op else []
    if op["kind"] == "IfElse":
        ids += [result["id"] for result in op["results"]]
    return ids


def all_value_ids(node, found):
    if is_value(node):
        found.add(node["id"])
    if isinstance(node, dict):
        for child in node.values():
            all_value_ids(child, found)
    elif isinstance(node, list):
        for child in node:
            all_value_ids(child, found)
    return found


def renamed(node, mapping):
    """A deep copy of `node` with the value ids replaced by `mapping`."""
    if is_value(node):
        return {**node, "id": mapping.get(node["id"], node["id"])}
    if isinstance(node, dict):
        return {key: renamed(child, mapping) for key, child in node.items()}
    if isinstance(node, list):
        return [renamed(child, mapping) for child in node]
    return node


def type_name(t):
    return t["name"] if t else "void"


def is_python_type(t):
    if t is None:
        return False
    if t["name"] in PYTHON_TYPES:
        return True
    if t["name"] == "record":
        return any(is_python_type(member) for member in t["members"].values())
    if t["name"] == "nullable":
        return is_python_type(t["type"])
    return False


def is_python_op(op):
    """Whether the op runs in CPython: a CPython operation, or a builtin that the Nautilus backend does not have."""
    kind = op["kind"]
    name = op.get("name", "")
    if kind == "CallBuiltin" and name not in NATIVE_BUILTINS:
        return True
    return kind.startswith("Py") or name.startswith("python.") or name.endswith("_python")


class Splitter:
    def __init__(self, module, entry):
        self.functions = {function["name"]: function for function in module["functions"]}
        self.entry = entry
        self.next_id = max(all_value_ids(module, set()) | {0}) + 1
        self.types = {}
        for function in module["functions"]:
            self.record_types(function)
        self.python_functions = {name for name in self.functions if self.function_uses_python(name, set())}

    def fresh(self, t):
        self.next_id += 1
        self.types[self.next_id] = t
        return {"kind": "value", "id": self.next_id, "type": t}

    def record_types(self, node):
        if is_value(node):
            self.types[node["id"]] = node["type"]
        if isinstance(node, dict):
            for child in node.values():
                self.record_types(child)
        elif isinstance(node, list):
            for child in node:
                self.record_types(child)

    # -- which functions need Python ----------------------------------------------------------------------------------

    def function_uses_python(self, name, seen):
        if name in seen or name not in self.functions:
            return False
        seen = seen | {name}
        return self.block_uses_python(self.functions[name]["body"], seen)

    def block_uses_python(self, ops, seen):
        for op in ops:
            if is_python_op(op) or any(is_python_type(self.types.get(i)) for i in uses(op) + defines(op)):
                return True
            if op["kind"] == "FunctionRef" and self.function_uses_python(op["name"], seen):
                return True
            if op["kind"] == "Call" and "name" in op and self.function_uses_python(op["name"], seen):
                return True
            if op["kind"] == "IfElse" and (self.block_uses_python(op["ifBody"], seen) or self.block_uses_python(op["elseBody"], seen)):
                return True
        return False

    # -- 1. inlining --------------------------------------------------------------------------------------------------

    def inline(self, ops, depth=0):
        """The ops with every direct call (and indirect call of a known function reference) replaced by the callee's body."""
        producers = {}
        out = []
        aliases = {}
        for op in ops:
            op = renamed(op, aliases) if aliases else op
            for i in defines(op):
                producers[i] = op
            if op["kind"] == "IfElse":
                op = {**op, "ifBody": self.inline(op["ifBody"], depth), "elseBody": self.inline(op["elseBody"], depth)}
            callee, arguments = None, None
            if op["kind"] == "Call" and "name" in op and "fnref" not in op:
                callee, arguments = op["name"], op["args"]
            elif op["kind"] == "Call" and is_value(op.get("fnref")):
                reference = producers.get(op["fnref"]["id"])
                if reference is not None and reference["kind"] == "FunctionRef":
                    callee = reference["name"]
                    arguments = op["args"] + ([reference["closure"]] if "closure" in reference else [])
            if callee is not None and callee in self.functions and depth < 16:
                function = self.functions[callee]
                mapping = {parameter["id"]: argument["id"] for parameter, argument in zip(function["args"], arguments)}
                for i in all_value_ids(function["body"], set()):
                    if i not in mapping:
                        mapping[i] = self.fresh(self.types.get(i))["id"]
                body = self.inline(renamed(function["body"], mapping), depth + 1)
                returned = None
                for inner in body:
                    if inner["kind"] == "Return":
                        returned = inner["values"][0] if inner["values"] else None
                    else:
                        out.append(inner)
                        for i in defines(inner):
                            producers[i] = inner
                if "result" in op and returned is not None:
                    alias = {"kind": "Alias", "args": [returned], "result": op["result"]}
                    out.append(alias)
                    producers[op["result"]["id"]] = alias
                continue
            out.append(op)
        return out

    # -- 2. if-conversion ---------------------------------------------------------------------------------------------

    def flatten(self, ops, guard, out):
        """Appends the ops, guarded by `guard` (a value id or None), to `out`; returns the values of a Yield or Return."""
        for op in ops:
            if op["kind"] in ("Yield", "Return"):
                return op["values"]
            if op["kind"] != "IfElse":
                out.append({**op, "guard": None if op["kind"] in FREE else guard})
                continue
            condition = op["cond"]
            boolean = {"kind": "type", "name": "bool"}
            then_guard = condition
            if guard is not None:
                then_guard = self.fresh(boolean)
                out.append({"kind": "CallBuiltin", "name": "scalar.bool.and", "args": [self.value(guard), condition],
                            "result": then_guard, "guard": None})
            negated = self.fresh(boolean)
            out.append({"kind": "CallBuiltin", "name": "scalar.bool.not", "args": [condition], "result": negated, "guard": None})
            else_guard = negated
            if guard is not None:
                else_guard = self.fresh(boolean)
                out.append({"kind": "CallBuiltin", "name": "scalar.bool.and", "args": [self.value(guard), negated],
                            "result": else_guard, "guard": None})
            then_values = self.flatten(op["ifBody"], then_guard["id"], out)
            else_values = self.flatten(op["elseBody"], else_guard["id"], out)
            for result, a, b in zip(op["results"], then_values, else_values):
                out.append({"kind": "Select", "args": [condition, a, b], "result": result, "guard": None})
        return None

    def value(self, i):
        return {"kind": "value", "id": i, "type": self.types.get(i)}

    # -- 3. classification ---------------------------------------------------------------------------------------------

    def classify(self, op):
        touched = [self.types.get(i) for i in uses(op) + defines(op)]
        if is_python_op(op) or any(is_python_type(t) for t in touched):
            return "python"
        if op["kind"] in FREE:
            return "free"
        if op["kind"] == "CallBuiltin" and op["name"] in LOOPS or op["kind"] == "Call":
            for i in uses(op):
                producer = self.producers.get(i)
                if producer is not None and producer["kind"] == "FunctionRef" and producer["name"] in self.python_functions:
                    return "python"
            if op["kind"] == "Call" and op.get("name") in self.python_functions:
                return "python"
        return "native"

    # -- 4./5. scheduling ------------------------------------------------------------------------------------------------

    def split(self):
        function = self.functions[self.entry]
        body = self.inline(function["body"])
        flat = []
        result_values = self.flatten(body, None, flat)
        result = result_values[0]["id"] if result_values else None
        self.producers = {i: op for op in flat for i in defines(op)}
        arguments = [argument["id"] for argument in function["args"]]

        # Dead code: ops whose values no one needs (Python ops stay: they can have side effects).
        needed = {result} if result is not None else set()
        live = []
        for op in reversed(flat):
            kind = self.classify(op)
            if kind == "python" and op["kind"] not in FREE or any(i in needed for i in defines(op)):
                live.append(op)
                needed.update(uses(op))
        flat = list(reversed(live))
        kinds = [self.classify(op) for op in flat]

        # The non-free ops each op depends on, through free ops.
        position = {i: index for index, op in enumerate(flat) for i in defines(op)}

        def real_dependencies(i, seen):
            if i not in position or i in seen:
                return set()
            seen.add(i)
            index = position[i]
            if kinds[index] != "free":
                return {index}
            found = set()
            for j in uses(flat[index]):
                found |= real_dependencies(j, seen)
            return found

        nodes = [index for index, kind in enumerate(kinds) if kind != "free"]
        dependencies = {index: set() for index in nodes}
        previous_python = None
        for index in nodes:
            for i in uses(flat[index]):
                dependencies[index] |= real_dependencies(i, set())
            if kinds[index] == "python":
                if previous_python is not None:
                    dependencies[index].add(previous_python)
                previous_python = index

        stage_of = {}
        stages = []  # (kind, [op indices])
        remaining = list(nodes)
        current = "native"
        while remaining:
            ready = [index for index in remaining if dependencies[index] <= stage_of.keys()]
            take = [index for index in ready if kinds[index] == current]
            if not take:
                current = "python" if current == "native" else "native"
                continue
            if not stages or stages[-1][0] != current:
                stages.append((current, []))
            while take:
                for index in take:
                    stage_of[index] = len(stages) - 1
                    stages[-1][1].append(index)
                    remaining.remove(index)
                ready = [index for index in remaining if dependencies[index] <= stage_of.keys()]
                take = [index for index in ready if kinds[index] == current]

        # A Python object lives only within Python stages: merge the stages that it would cross.
        merges = 0
        changed = True
        while changed:
            changed = False
            for index in nodes:
                for i in defines(flat[index]):
                    if not is_python_type(self.types.get(i)):
                        continue
                    users = [user for user in nodes if i in self.uses_through_free(flat, position, kinds, user)]
                    last = max([stage_of[user] for user in users] + [len(stages) - 1 if i == result else -1])
                    first = stage_of[index]
                    if any(stages[s][0] == "native" for s in range(first, last + 1)):
                        merged = sorted(op for s in range(first, last + 1) for op in stages[s][1])
                        stages[first:last + 1] = [("python", merged)]
                        merges += 1
                        changed = True
                        stage_of = {op: s for s, (_, ops) in enumerate(stages) for op in ops}
                        break
                if changed:
                    break
            # Neighbours of the same kind become one stage.
            joined = []
            for kind, ops in stages:
                if joined and joined[-1][0] == kind:
                    joined[-1] = (kind, sorted(joined[-1][1] + ops))
                else:
                    joined.append((kind, ops))
            stages = joined
            stage_of = {op: s for s, (_, ops) in enumerate(stages) for op in ops}

        return self.describe(flat, kinds, position, stages, stage_of, arguments, result, merges)

    def uses_through_free(self, flat, position, kinds, index, seen=None):
        """The values that op `index` reads, also through the free ops it reads (which its stage recomputes)."""
        seen = set() if seen is None else seen
        found = set()
        for i in uses(flat[index]):
            if i in seen:
                continue
            seen.add(i)
            found.add(i)
            if i in position and kinds[position[i]] == "free":
                found |= self.uses_through_free(flat, position, kinds, position[i], seen)
        return found

    def describe(self, flat, kinds, position, stages, stage_of, arguments, result, merges):
        """The chain: per stage its ops (with the free ops it recomputes), inputs and outputs."""
        chain = []
        needed_later = {}
        stage_needs = []
        for kind, indices in stages:
            needs = set()
            free_ops = set()
            for index in indices:
                for i in self.uses_through_free(flat, position, kinds, index):
                    if i in position and kinds[position[i]] == "free":
                        free_ops.add(position[i])
                    else:
                        needs.add(i)
            stage_needs.append((needs, free_ops))
        for s, (kind, indices) in enumerate(stages):
            defined = {i for index in indices for i in defines(flat[index])}
            needs, free_ops = stage_needs[s]
            if s == len(stages) - 1 and result is not None:
                needs = needs | {result}
                if result in position and kinds[position[result]] == "free":
                    free_ops = free_ops | {position[result]} | {position[i] for i in
                                                                  self.uses_through_free(flat, position, kinds, position[result])
                                                                  if i in position and kinds[position[i]] == "free"}
                    needs = needs | {i for i in self.uses_through_free(flat, position, kinds, position[result])
                                     if not (i in position and kinds[position[i]] == "free")}
            inputs = sorted(i for i in needs if i not in defined and not (i in position and kinds[position[i]] == "free"))
            later = set()
            for t in range(s + 1, len(stages)):
                later |= stage_needs[t][0]
            if result is not None:
                later.add(result)
            outputs = sorted(i for i in defined if i in later)
            ops = sorted(set(indices) | free_ops)
            chain.append({
                "kind": kind,
                "inputs": [{"id": i, "type": self.types.get(i)} for i in inputs],
                "outputs": [{"id": i, "type": self.types.get(i)} for i in outputs],
                "ops": [flat[index] for index in ops],
            })
        if not chain:
            # Only free ops (the result is a constant or an argument): one native stage computes it.
            free_ops, inputs = set(), set()
            if result in position:
                free_ops.add(position[result])
                for i in self.uses_through_free(flat, position, kinds, position[result]):
                    (free_ops.add(position[i]) if i in position else inputs.add(i))
            elif result is not None:
                inputs.add(result)
            chain.append({"kind": "native", "inputs": [{"id": i, "type": self.types.get(i)} for i in sorted(inputs)], "outputs": [],
                          "ops": [flat[index] for index in sorted(free_ops)]})
        # A field stays in the record after the stage that wrote it, so a value from an earlier stage (or an argument) is an
        # input of the last stage, which outputs the result.
        if result is not None and result not in {o["id"] for o in chain[-1]["outputs"]}:
            chain[-1]["outputs"].append({"id": result, "type": self.types.get(result)})
        return {"arguments": [{"id": i, "type": self.types.get(i)} for i in arguments],
                "result": result, "merges": merges, "stages": chain}


def split(module, entry):
    """The chain of stages of the entry function of a serialized HiPy module."""
    return Splitter(copy.deepcopy(module), entry).split()


# ---------------------------------------------------------------------------------------------------------------------------
# Python code for stages and helper functions.
# ---------------------------------------------------------------------------------------------------------------------------

PRELUDE = '''import builtins as _builtins
import importlib as _importlib
import math as _math
import types as _types

_main = _types.ModuleType("__hipy_main__")
exec(_SETUP, _main.__dict__)
_main.__builtins__ = _builtins


def _import(name):
    return _main if name == "__main__" else _importlib.import_module(name)


def _clamp(s, i):
    return max(0, min(i, len(s)))


def _substr(s, a, b):
    a, b = _clamp(s, a), _clamp(s, b)
    return s[a:b] if b > a else ""


def _find(s, sub, a, b):
    a = _clamp(s, a)
    return s.find(sub, a, max(_clamp(s, b), a))


def _rfind(s, sub, a, b):
    a = _clamp(s, a)
    return s.rfind(sub, a, max(_clamp(s, b), a))


def _lower(s):
    return "".join(chr(ord(c) + 32) if "A" <= c <= "Z" else c for c in s)


def _upper(s):
    return "".join(chr(ord(c) - 32) if "a" <= c <= "z" else c for c in s)


def _fdiv(a, b):
    if b == 0:
        return float("nan") if a == 0 or a != a else _math.copysign(_math.inf, a) * _math.copysign(1.0, b)
    return a / b


def _as_int(x):
    if not isinstance(x, int):
        raise TypeError("an integer is required, got " + type(x).__name__)
    return int(x)


def _as_float(x):
    if isinstance(x, str):
        raise TypeError("must be real number, not str")
    return float(x)


def _as_str(x):
    if not isinstance(x, str):
        raise TypeError("expected str, got " + type(x).__name__)
    return x


def _get_value(x):
    if x is None:
        raise RuntimeError("get_value() on NULL")
    return x


def _error(message):
    raise RuntimeError("HiPy UDF raised: " + message)


_NONE = object()


def _call(reference, *arguments):
    name, closure = reference
    function = globals()["f_" + name]
    return function(*arguments) if closure is _NONE else function(*arguments, closure)


def _range_iter(reference, closure, state, start, stop, step):
    for i in range(start, stop, step):
        state = _call(reference, closure, state, i)
    return state


def _string_iter(reference, closure, state, text):
    for c in text:
        state = _call(reference, closure, state, c)
    return state


def _list_set(container, key, value):
    container[key] = value


def _list_sort(items, less):
    import functools
    items.sort(key=functools.cmp_to_key(lambda a, b: -1 if _call(less, a, b) else (1 if _call(less, b, a) else 0)))


def _regex_search(pattern, text):
    import re
    match = re.search(pattern, text)
    if match is None:
        return []
    return [{"_elt0": match.start(g), "_elt1": match.end(g)} for g in range(match.re.groups + 1)]


def _try_except(attempt, fallback):
    try:
        return _call(attempt)
    except Exception:
        return _call(fallback)


def _while_iter(condition, body, closure, state):
    while _call(condition, closure, state):
        state = _call(body, closure, state)
    return state
'''

BINARY = {
    "scalar.int.add": "({0} + {1})", "scalar.int.sub": "({0} - {1})", "scalar.int.mul": "({0} * {1})",
    "scalar.int.div": "({0} // {1})", "scalar.int.mod": "({0} % {1})", "scalar.int.lshift": "({0} << {1})",
    "scalar.int.and": "({0} & {1})", "scalar.int.xor": "({0} ^ {1})",
    "scalar.float.add": "({0} + {1})", "scalar.float.sub": "({0} - {1})", "scalar.float.mul": "({0} * {1})",
    "scalar.float.div": "_fdiv({0}, {1})", "scalar.float.mod": "({0} % {1})", "scalar.float.pow": "({0} ** {1})",
    "scalar.float.atan2": "_math.atan2({0}, {1})", "scalar.float.copysign": "_math.copysign({0}, {1})",
    "scalar.bool.and": "({0} and {1})", "scalar.bool.or": "({0} or {1})",
    "scalar.string.concatenate": "({0} + {1})", "scalar.string.contains": "({1} in {0})",
    "scalar.string.compare.eq": "({0} == {1})", "scalar.string.compare.lt": "({0} < {1})", "scalar.string.compare.lte": "({0} <= {1})",
    "python.operator.add": "({0} + {1})", "python.operator.sub": "({0} - {1})", "python.operator.mul": "({0} * {1})",
    "python.operator.div": "({0} / {1})", "python.operator.mod": "({0} % {1})", "python.operator.pow": "({0} ** {1})",
    "python.operator.lt": "({0} < {1})", "python.operator.gt": "({0} > {1})", "python.operator.eq": "({0} == {1})",
    "python.operator.contains": "({1} in {0})",
}
for _t in ("int", "float"):
    for _p, _o in (("eq", "=="), ("neq", "!="), ("lt", "<"), ("lte", "<="), ("gt", ">"), ("gte", ">=")):
        BINARY[f"scalar.{_t}.compare.{_p}"] = "({0} " + _o + " {1})"
UNARY = {
    "scalar.bool.not": "(not {0})", "scalar.float.neg": "(-{0})", "scalar.float.from_int": "float({0})",
    "scalar.float.to_int": "int({0})", "scalar.float.to_string": "repr({0})", "scalar.float.from_string": "float({0})",
    "scalar.int.to_string": "str({0})", "scalar.int.from_string": "int({0})", "scalar.int.pyint_to_int64": "{0}",
    "scalar.float.exp": "_math.exp({0})", "scalar.float.log": "_math.log({0})", "scalar.float.sqrt": "_math.sqrt({0})",
    "scalar.float.sin": "_math.sin({0})", "scalar.float.cos": "_math.cos({0})", "scalar.float.acos": "_math.acos({0})",
    "scalar.string.length": "len({0})", "scalar.string.lower": "_lower({0})", "scalar.string.upper": "_upper({0})",
    "scalar.string.ord": "ord({0})", "scalar.string.strip": "{0}.strip(' \\t\\n\\r\\f\\v')",
    "nullable.make": "{0}", "nullable.is_null": "({0} is None)", "nullable.get_value": "_get_value({0})",
    "error": "_error({0})",
    "scalar.int.to_python": "{0}", "scalar.float.to_python": "{0}", "scalar.bool.to_python": "{0}",
    "scalar.string.to_python": "{0}", "scalar.int.from_python": "_as_int({0})", "scalar.float.from_python": "_as_float({0})",
    "scalar.bool.from_python": "bool({0})", "scalar.string.from_python": "_as_str({0})",
    "python.operator.neg": "(-{0})", "python.operator.inv": "(~{0})", "python.tuple_from_list": "tuple({0})",
}
OTHER = {
    "scalar.string.substr": "_substr({0}, {1}, {2})", "scalar.string.at": "_substr({0}, {1}, {1} + 1)",
    "scalar.string.find": "_find({0}, {1}, {2}, {3})", "scalar.string.rfind": "_rfind({0}, {1}, {2}, {3})",
    "scalar.string.replace": "{0}.replace({1}, {2})", "python.create_slice": "slice({0}, {1}, {2})",
    "python.get_none": "None", "python.create_list": "[]", "python.create_dict": "{{}}",
    "nullable.null": "None", "undef": "None",
    "range.iter": "_range_iter({0}, {1}, {2}, {3}, {4}, {5})", "scalar.string.iter": "_string_iter({0}, {1}, {2}, {3})",
    "while.iter": "_while_iter({0}, {1}, {2}, {3})", "list.iter": "_string_iter({0}, {1}, {2}, {3})",
    "list.create": "[]", "list.append": "{0}.append({1})", "list.at": "{0}[{1}]", "list.set": "_list_set({0}, {1}, {2})",
    "list.length": "len({0})", "list.pop": "{0}.pop({1})", "list.sort": "_list_sort({0}, {1})",
    "dict.create": "{{}}", "dict.set": "_list_set({0}, {1}, {2})", "dict.get": "{0}[{1}]", "dict.contains": "({1} in {0})",
    "dict.length": "len({0})", "dict.iter_keys": "_string_iter({0}, {1}, {2}, list({3}))",
    "dict.iter_items": "_string_iter({0}, {1}, {2}, [{{'_elt0': k, '_elt1': x}} for k, x in {3}.items()])",
    "scalar.string.split": "{0}.split({1}, {2})", "scalar.string.format_single": "{0}.format({1})",
    "regex.search": "_regex_search({0}, {1})", "try_except": "_try_except({0}, {1})", "dbg.print": "print({0})",
}


def v(value):
    return f"v{value['id'] if isinstance(value, dict) else value}"


def builtin_expression(op):
    name = op["name"]
    args = [v(a) for a in op["args"]]
    result_type = type_name(op["result"]["type"]) if "result" in op else "void"
    if name == "scalar.float.ceil":
        return f"_math.ceil({args[0]})" if result_type in ("int", "pyint") else f"float(_math.ceil({args[0]}))"
    if name == "scalar.float.round":
        return f"int(round({args[0]}, {args[1]}))" if result_type in ("int", "pyint") else f"float(round({args[0]}, {args[1]}))"
    for table in (BINARY, UNARY, OTHER):
        if name in table:
            return table[name].format(*args)
    raise NotImplementedError(f"no Python code for {name}")


def constant(op):
    value = op["value"]
    if type_name(op["result"]["type"]) == "float":
        return f"float({value!r})"
    return repr(value)


def statements(op, indent):
    """The Python statements of one op (structured: an IfElse becomes if/else)."""
    pad = "    " * indent
    kind = op["kind"]
    if kind == "IfElse":
        lines = [f"{pad}if {v(op['cond'])}:"]
        for body in ("ifBody", "elseBody"):
            if body == "elseBody":
                lines.append(f"{pad}else:")
            inner = []
            for child in op[body]:
                if child["kind"] == "Yield":
                    inner += [f"{pad}    {v(r)} = {v(y)}" for r, y in zip(op["results"], child["values"])]
                else:
                    inner += statements(child, indent + 1)
            lines += inner or [f"{pad}    pass"]
        return lines
    if kind == "Return":
        return [f"{pad}return {v(op['values'][0]) if op['values'] else 'None'}"]
    if kind == "NoOp":
        return []
    if kind == "PySetAttr":
        return [f"{pad}setattr({v(op['on'])}, {op['name']!r}, {v(op['value'])})"]
    if kind == "Constant":
        expression = constant(op)
    elif kind == "CallBuiltin":
        expression = builtin_expression(op)
    elif kind == "Call":
        args = ", ".join(v(a) for a in op["args"])
        expression = f"_call({v(op['fnref'])}{', ' if args else ''}{args})" if is_value(op.get("fnref")) else f"f_{op['name']}({args})"
    elif kind == "FunctionRef":
        expression = f"({op['name']!r}, {v(op['closure']) if 'closure' in op else '_NONE'})"
    elif kind == "MakeRecord":
        expression = "{" + ", ".join(f"{name!r}: {v(value)}" for name, value in op["values"].items()) + "}"
    elif kind == "RecordGet":
        # Free ops run unguarded: a record that a guard did not compute is None, and so are its members.
        expression = f"({v(op['record'])} or {{}}).get({op['member']!r})"
    elif kind == "Undef":
        expression = "None"
    elif kind == "PyImport":
        expression = f"_import({op['name']!r})"
    elif kind == "PyGetAttr":
        expression = f"getattr({v(op['on'])}, {op['name']!r})"
    elif kind == "PyCall2":
        args = [v(a) for a in op["args"]] + [f"{k['name']}={v(k['value'])}" for k in op["kw_args"]]
        expression = f"{v(op['callable'])}({', '.join(args)})"
    elif kind == "Select":
        expression = f"({v(op['args'][1])} if {v(op['args'][0])} else {v(op['args'][2])})"
    elif kind == "Alias":
        expression = v(op["args"][0])
    else:
        raise NotImplementedError(f"no Python code for {kind}")
    line = f"{pad}{v(op['result'])} = {expression}" if "result" in op else f"{pad}{expression}"
    if op.get("guard") is not None:
        return [f"{pad}if v{op['guard']}:", "    " + line]
    return [line]


def function_source(function):
    parameters = ", ".join(v(a) for a in function["args"])
    lines = [f"def f_{function['name']}({parameters}):"]
    for op in function["body"]:
        lines += statements(op, 1)
    return "\n".join(lines + ["    return None"]) + "\n"


def stage_source(stage, name):
    """A function `name(values)` that computes the stage from a dict of its inputs and returns a dict of its outputs."""
    lines = [f"def {name}(values):"]
    lines += [f"    {v(i['id'])} = values[{i['id']}]" for i in stage["inputs"]]
    defined = sorted({i for op in stage["ops"] for i in defines(op)} - {i["id"] for i in stage["inputs"]})
    lines += [f"    v{i} = None" for i in defined]
    for op in stage["ops"]:
        lines += statements(op, 1)
    lines.append("    return {" + ", ".join(f"{o['id']}: {v(o['id'])}" for o in stage["outputs"]) + "}")
    return "\n".join(lines) + "\n"


def module_source(module, chain, setup):
    """Python source with the prelude, every function of the module, and one function per stage (stage_0, stage_1, ...)."""
    parts = [f"_SETUP = {setup!r}\n", PRELUDE]
    for function in module["functions"]:
        parts.append(function_source(function))
    for index, stage in enumerate(chain["stages"]):
        parts.append(stage_source(stage, f"stage_{index}"))
    return "\n\n".join(parts)


def python_setup(module):
    lines = [f"import {name} as {alias}" for alias, name in module.get("imports", {}).items()]
    lines += list(module.get("py_functions", {}).values())
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    import sys

    compiled = json.load(open(sys.argv[1], encoding="utf-8"))
    chain = split(compiled["module"], compiled["entry"])
    print(" -> ".join(f"{s['kind']}[{len(s['ops'])} ops, in {len(s['inputs'])}, out {len(s['outputs'])}]" for s in chain["stages"]))


# ---------------------------------------------------------------------------------------------------------------------------
# Lowering: one UDF call per stage output, for HiPyChainRule (nes-query-optimizer).
# ---------------------------------------------------------------------------------------------------------------------------

NES_TYPES = {"int": "INT64", "pyint": "INT64", "float": "FLOAT64", "bool": "BOOLEAN", "string": "VARSIZED"}


def nes_type(t):
    """The NES type of a field that holds a value of HiPy type `t`, or None if a field cannot hold it."""
    name = type_name(t)
    if name == "nullable":
        return nes_type(t["type"])
    return NES_TYPES.get(name)


def referenced_functions(ops, functions, found):
    """The functions that `ops` reference (function references and calls), transitively."""
    for op in ops:
        for name in [op.get("name")] if op["kind"] in ("FunctionRef", "Call") else []:
            if name in functions and name not in found:
                found.add(name)
                referenced_functions(functions[name]["body"], functions, found)
        for body in ("ifBody", "elseBody"):
            if body in op:
                referenced_functions(op[body], functions, found)
    return found


def slice_for(ops, output):
    """The ops that compute `output` (and every error op, which raises): the native program of one output."""
    needed = {output}
    kept = []
    for op in reversed(ops):
        if any(i in needed for i in defines(op)) or op.get("name") == "error":
            kept.append(op)
            needed.update(uses(op))
    return list(reversed(kept))


def lowering(chain, module, entry, argument_types, return_type):
    """Adds the calls that compute each stage output to `chain`, and whether the chain can be lowered (`lowerable`)."""
    stages = chain["stages"]
    arguments = [a["id"] for a in chain["arguments"]]
    declared = dict(zip(arguments, argument_types))
    if not any(stage["kind"] == "python" for stage in stages):
        chain["lowerable"], chain["reason"] = False, "no Python stage: the UDF runs natively as one unit"
        return chain
    defined_by = {o["id"]: (index, stage["kind"]) for index, stage in enumerate(stages) for o in stage["outputs"]}
    for index, stage in enumerate(stages):
        for value in stage["inputs"] + (stage["outputs"] if index < len(stages) - 1 else []):
            if value["id"] not in declared and nes_type(value["type"]) is None:
                chain["lowerable"] = False
                chain["reason"] = f"value %{value['id']} of type {type_name(value['type'])} crosses a cut"
                return chain
    functions = {function["name"]: function for function in module["functions"]}
    source = chain["pythonSource"]
    digest = __import__("hashlib").sha256(source.encode()).hexdigest()[:16]
    last = len(stages) - 1

    types = {value["id"]: value["type"] for stage in stages for value in stage["inputs"] + stage["outputs"]}
    types.update({a["id"]: a["type"] for a in chain["arguments"]})

    def type_of(i):
        return nes_type(types[i])

    for index, stage in enumerate(stages):
        inputs = [value["id"] for value in stage["inputs"]]
        calls = {}
        for output in stage["outputs"]:
            o = output["id"]
            result_type = return_type if o == chain["result"] else type_of(o)
            if stage["kind"] == "native":
                # A NULL argument of the UDF makes the result NULL; a value that a guard did not compute arrives as NULL from
                # a Python stage and is a placeholder here. The arguments of the UDF are passed too, so that a NULL argument
                # also nulls the later native stages.
                parameters = inputs + [a for a in arguments if a not in inputs]
                body = slice_for(stage["ops"], o)
                names = referenced_functions(body, functions, set())
                program = {
                    "entry": f"stage_{index}_{o}",
                    "module": {"kind": "module", "imports": {}, "py_functions": {}, "functions": [
                        {"kind": "function", "name": f"stage_{index}_{o}",
                         "args": [{"kind": "value", "id": i, "type": types[i]} for i in parameters],
                         "res_type": types[o], "body": body + [{"kind": "Return", "values": [{"kind": "value", "id": o, "type": types[o]}]}]},
                        *[functions[name] for name in sorted(names)]]},
                    "nullPropagates": [i in declared for i in parameters],
                }
                calls[o] = {"kind": "native", "arguments": parameters, "argumentTypes": [declared.get(i) or type_of(i) for i in parameters],
                            "notNull": [False] * len(parameters), "returnType": result_type, "program": json.dumps(program)}
            else:
                # An argument of the UDF or a value of a native stage is NULL only if an argument of the UDF is: then the call
                # returns NULL without running. A value of an earlier Python stage that a guard did not compute is None.
                parameters = inputs
                body = "\n".join([
                    "import builtins",
                    "chains = builtins.__dict__.setdefault('__hipy_chains__', {})",
                    f"chain = chains.get({digest!r})",
                    "if chain is None:",
                    f"    chain = {{'__name__': '__hipy_chain_{digest}'}}",
                    f"    exec(compile({source!r}, '<hipy chain {digest}>', 'exec'), chain)",
                    f"    chains[{digest!r}] = chain",
                    f"return chain['stage_{index}']({{{', '.join(f'{i}: a{n}' for n, i in enumerate(parameters))}}})[{o}]",
                ])
                calls[o] = {"kind": "python", "arguments": parameters, "argumentTypes": [declared.get(i) or type_of(i) for i in parameters],
                            "notNull": [i in declared or defined_by.get(i, (0, "native"))[1] == "native" for i in parameters],
                            "returnType": result_type, "parameterNames": [f"a{n}" for n in range(len(parameters))], "body": body}
        stage["calls"] = calls
    chain["lowerable"] = True
    return chain
