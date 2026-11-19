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
"""A front end for numba.njit that rewrites a UDF's syntax tree before Numba compiles it, for Python that Numba 0.68 does not
compile. The source file stays as it is. Each rewrite keeps the result of the UDF:

- A generator expression becomes a list comprehension (Numba does not compile a generator expression passed to a call).
- A nested function without free variables becomes a jitted function of its own (an exception raised in a nested function
  that is called inside try/except crashes Numba 0.68.1).
- An import inside the function moves to module level (Numba does not compile import statements).
- In a tuple of tuples (a constant table), an empty list in a column of string lists becomes an empty list of strings,
  and `x == []` becomes `len(x) == 0` (Numba cannot type an empty list literal).
- The test `if x:` (also `while x:`, `... if x else ...` and `not x`) of a variable becomes `x is not None and x`. Numba
  0.68.1 raises TypeError for the truth value of an optional string that is None.
- `type(x) == list` (also tuple, dict, str) becomes `isinstance(x, list)`. Numba does not compare types.
- In a function that asks a JSON value for its kind (is_list(), is_dict(), ...), json.loads(x) becomes json_value(x)
  (json_value_ext).
- A plain Python function of the UDF's module that the UDF calls (a helper) is compiled with this front end too.
- A global set or frozenset of strings becomes a sorted string blob with an offset array, and `x in s` / `x not in s`
  becomes a binary search on it (Numba does not freeze sets as constants, and freezes tuples only up to 1000 items).
"""

import ast
import inspect
import textwrap

import numba
import numpy

from nes_numba_ext.json_value_ext import json_value

from numba.core.extending import register_jitable


@register_jitable
def _frozen_contains(frozen, word):
    blob, offsets = frozen
    low, high = 0, len(offsets) - 1
    while low < high:
        middle = (low + high) // 2
        item = blob[offsets[middle]:offsets[middle + 1]]
        if item == word:
            return True
        if item < word:
            low = middle + 1
        else:
            high = middle
    return False


def _is_string_set(value):
    return isinstance(value, (set, frozenset)) and value and all(isinstance(item, str) for item in value)


def _freeze(value):
    items = sorted(value)
    offsets = numpy.zeros(len(items) + 1, dtype=numpy.int64)
    for index, item in enumerate(items):
        offsets[index + 1] = offsets[index] + len(item)
    return "".join(items), offsets


class _TypeComparisons(ast.NodeTransformer):
    def visit_Compare(self, node):
        self.generic_visit(node)
        left, right = node.left, node.comparators[0]
        is_type_call = isinstance(left, ast.Call) and isinstance(left.func, ast.Name) and left.func.id == "type" and len(left.args) == 1
        if len(node.ops) == 1 and isinstance(node.ops[0], (ast.Eq, ast.Is)) and is_type_call and isinstance(right, ast.Name) \
                and right.id in ("list", "tuple", "dict", "str"):
            call = ast.Call(func=ast.Name(id="isinstance", ctx=ast.Load()), args=[left.args[0], right], keywords=[])
            return ast.copy_location(call, node)
        return node


_KIND_METHODS = {"is_null", "is_bool", "is_int", "is_float", "is_number", "is_str", "is_list", "is_dict"}


def _asks_json_kind(function):
    return any(isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in _KIND_METHODS
               for node in ast.walk(function))


class _JsonValues(ast.NodeTransformer):
    def visit_Call(self, node):
        self.generic_visit(node)
        target = node.func
        if isinstance(target, ast.Attribute) and target.attr == "loads" and isinstance(target.value, ast.Name) \
                and target.value.id == "json":
            node.func = ast.copy_location(ast.Name(id="_nes_json_value", ctx=ast.Load()), target)
        return node


def _guarded(test):
    if isinstance(test, ast.Name):
        guard = ast.Compare(left=ast.Name(id=test.id, ctx=ast.Load()), ops=[ast.IsNot()], comparators=[ast.Constant(None)])
        return ast.copy_location(ast.BoolOp(op=ast.And(), values=[guard, test]), test)
    if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not) and isinstance(test.operand, ast.Name):
        return ast.copy_location(ast.UnaryOp(op=ast.Not(), operand=_guarded(test.operand)), test)
    return test


class _OptionalTruth(ast.NodeTransformer):
    def _visit_test(self, node):
        self.generic_visit(node)
        node.test = _guarded(node.test)
        return node

    visit_If = visit_While = visit_IfExp = _visit_test


@register_jitable
def _empty_string_list():
    result = [""]
    result.pop()
    return result


class _EmptyLists(ast.NodeTransformer):
    def visit_Tuple(self, node):
        self.generic_visit(node)
        rows = node.elts
        if len(rows) < 2 or not all(isinstance(row, ast.Tuple) and len(row.elts) == len(rows[0].elts) for row in rows):
            return node
        for column in range(len(rows[0].elts)):
            cells = [row.elts[column] for row in rows]
            if not all(isinstance(cell, ast.List) for cell in cells):
                continue
            items = [item for cell in cells for item in cell.elts]
            if items and all(isinstance(item, ast.Constant) and isinstance(item.value, str) for item in items):
                for row in rows:
                    if not row.elts[column].elts:
                        row.elts[column] = ast.Call(func=ast.Name(id="_nes_empty_string_list", ctx=ast.Load()), args=[], keywords=[])
        return node

    def visit_Compare(self, node):
        self.generic_visit(node)
        if len(node.ops) == 1 and isinstance(node.ops[0], (ast.Eq, ast.NotEq)):
            right = node.comparators[0]
            if isinstance(right, ast.List) and not right.elts:
                length = ast.Call(func=ast.Name(id="len", ctx=ast.Load()), args=[node.left], keywords=[])
                return ast.copy_location(ast.Compare(left=length, ops=[node.ops[0]], comparators=[ast.Constant(0)]), node)
        return node


class _SetMembership(ast.NodeTransformer):
    """`x in s` and `x not in s` for a global string set `s` become `_frozen_contains(s, x)`."""

    def __init__(self, names):
        self.names = names

    def visit_Compare(self, node):
        self.generic_visit(node)
        if len(node.ops) != 1 or not isinstance(node.ops[0], (ast.In, ast.NotIn)):
            return node
        target = node.comparators[0]
        if not (isinstance(target, ast.Name) and target.id in self.names):
            return node
        call = ast.Call(func=ast.Name(id="_nes_frozen_contains", ctx=ast.Load()), args=[target, node.left], keywords=[])
        if isinstance(node.ops[0], ast.NotIn):
            call = ast.UnaryOp(op=ast.Not(), operand=call)
        return ast.copy_location(call, node)


class _GeneratorsToLists(ast.NodeTransformer):
    def visit_GeneratorExp(self, node):
        self.generic_visit(node)
        return ast.copy_location(ast.ListComp(elt=node.elt, generators=node.generators), node)


def _assigned_names(function):
    """Parameters and names that a function assigns, without those of functions nested in it."""
    names = {argument.arg for argument in function.args.args + function.args.kwonlyargs}
    stack = list(function.body)
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.Lambda)):
            if isinstance(node, ast.FunctionDef):
                names.add(node.name)
            continue
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            names.add(node.id)
        stack.extend(ast.iter_child_nodes(node))
    return names


def _loaded_names(function):
    return {node.id for node in ast.walk(function) if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)}


def _lift(function, lifted):
    """Moves the nested functions of `function` that use no variable of it to `lifted`, innermost first."""
    outer = _assigned_names(function)
    nested = [node for node in function.body if isinstance(node, ast.FunctionDef)]
    for node in nested:
        _lift(node, lifted)
    # A nested function stays if it uses a variable of `function`, or a nested function that stays.
    staying = set()
    changed = True
    while changed:
        changed = False
        for node in nested:
            if node.name in staying:
                continue
            free = (_loaded_names(node) - _assigned_names(node)) & outer
            if free & (staying | (outer - {other.name for other in nested})):
                staying.add(node.name)
                changed = True
    lifted.extend(node for node in nested if node.name not in staying)
    function.body = [node for node in function.body if not (isinstance(node, ast.FunctionDef) and node.name not in staying)]
    function.body = function.body or [ast.Pass()]


def _hoist_imports(function, imports):
    body = []
    for node in function.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            imports.append(node)
        else:
            body.append(node)
    function.body = body or [ast.Pass()]
    for node in ast.walk(function):
        if node is not function and isinstance(node, ast.FunctionDef):
            _hoist_imports(node, imports)


def njit(function, _compiled=None):
    """numba.njit of `function` after the rewrites above."""
    compiled = {} if _compiled is None else _compiled
    if function in compiled:
        return compiled[function]
    tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
    definition = tree.body[0]
    imports = []
    _hoist_imports(definition, imports)
    _GeneratorsToLists().visit(definition)
    _EmptyLists().visit(definition)
    _OptionalTruth().visit(definition)
    _TypeComparisons().visit(definition)
    if _asks_json_kind(definition):
        _JsonValues().visit(definition)
    lifted = []
    _lift(definition, lifted)
    sets = {name for name, value in function.__globals__.items() if _is_string_set(value)}
    namespace = {name: _freeze(value) if name in sets else value for name, value in function.__globals__.items()}
    namespace["_nes_frozen_contains"] = _frozen_contains
    namespace["_nes_empty_string_list"] = _empty_string_list
    namespace["_nes_json_value"] = json_value
    # Helpers: plain functions of the UDF's module that it uses, compiled with this front end. A placeholder in `compiled`
    # stops a cycle; Numba resolves the name at compile time, after every helper is in the namespace.
    compiled[function] = None
    for name in _loaded_names(definition):
        value = namespace.get(name)
        if inspect.isfunction(value) and value is not function and value.__module__ == function.__module__:
            namespace[name] = njit(value, compiled)
    for node in lifted + [definition]:
        _SetMembership(sets).visit(node)
    for node in imports:
        exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), "<nes_numba_ext>", "exec"), namespace)
    # Lifted functions are jitted in dependency order: a later one may call an earlier one through the namespace.
    for node in lifted:
        exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), inspect.getsourcefile(function), "exec"), namespace)
        namespace[node.name] = numba.njit(namespace[node.name])
    exec(compile(ast.fix_missing_locations(ast.Module(body=[definition], type_ignores=[])), inspect.getsourcefile(function), "exec"), namespace)
    compiled[function] = numba.njit(namespace[definition.name])
    return compiled[function]
