# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# NES Query Plan Pretty Printers for GDB
#
# Two complementary features:
#
# 1) LABELS (automatic): LogicalPlan / LogicalOperator / LogicalFunction keep
#    GDB's DEFAULT expandable structure completely unchanged; we only add an
#    informative one-line label, e.g.
#        sinkOp = {NES::LogicalOperator} Sink (id: 7, children: [Selection])
#    Labels are computed by passive memory reading only (no C++ calls, no
#    copies), so they cannot crash the inferior, even on half-built objects.
#
# 2) SHALLOW VIEW (manual): the C++ helper NES::Debug::view(plan) returns a
#    lightweight PlanView/OperatorView tree. Type it in the Watches /
#    Evaluate-expression box to get a compact operator tree without the
#    self/get()/impl indirection. The printers below render those views as
#    label + children directly.
#
# Setup: loaded automatically via the project .gdbinit. No setup needed.

import gdb


# ---------------------------------------------------------------------------
# Safe, passive memory reads (never execute code in the inferior)
# ---------------------------------------------------------------------------

def _looks_like_heap_ptr(addr):
    try:
        return int(addr) >= 0x10000
    except Exception:
        return False


def _name_from_type(erased):
    """OperatorModel<NES::SourceNameLogicalOperator> -> SourceNameLogicalOperator"""
    try:
        tname = str(erased.dynamic_type)
        inner = tname[tname.index('<') + 1: tname.rindex('>')]
        return inner.split('::')[-1].strip()
    except Exception:
        return None


def _read_name(impl, erased):
    """Operator/function display name: static NAME if present, else type name."""
    try:
        name_sv = impl['NAME']
        length = int(name_sv['_M_len'])
        data = name_sv['_M_str']
        if _looks_like_heap_ptr(data) and 0 < length < 256:
            return data.string(length=length)
    except Exception:
        pass
    return _name_from_type(erased)


def _read_std_string(val):
    """Read a std::string gdb.Value into a python str, or None.

    Delegates to the registered std::string pretty-printer so it works under
    both libc++ (local build) and libstdc++ (remote build), whose internal
    layouts differ; falls back to raw libstdc++ field access."""
    try:
        vis = gdb.default_visualizer(val)
        if vis is not None:
            s = vis.to_string()
            if hasattr(s, 'string'):
                s = s.string()
            if s is not None:
                s = str(s)
                # std::string visualizers hand back a gdb LazyString that
                # stringifies with GDB's surrounding double quotes; strip one pair.
                if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
                    s = s[1:-1]
                return s
    except Exception:
        pass
    try:
        p = val['_M_dataplus']['_M_p']
        if not _looks_like_heap_ptr(p):
            return None
        length = int(val['_M_string_length'])
        if length < 0 or length > 4096:
            return None
        return p.string(length=length)
    except Exception:
        return None


def _resolve_impl(handle_val):
    """Follow handle.self (shared_ptr) to the concrete model. None if unsafe."""
    try:
        shared = handle_val['self']
        ptr = shared['_M_ptr']
        if not _looks_like_heap_ptr(ptr):
            return None
        erased = ptr.dereference()
        concrete = erased.cast(erased.dynamic_type)
        return (concrete['impl'], erased, concrete)
    except Exception:
        return None


def _vector_range(vec_val, max_count=4096):
    """Yield elements of a std::vector gdb.Value, with sanity checks.

    Delegates to the registered std::vector pretty-printer (works under both
    libc++ and libstdc++); falls back to raw libstdc++ field access."""
    try:
        vis = gdb.default_visualizer(vec_val)
    except Exception:
        vis = None
    if vis is not None and hasattr(vis, 'children'):
        count = 0
        for _, elem in vis.children():
            yield elem
            count += 1
            if count >= max_count:
                return
        return
    try:
        impl = vec_val['_M_impl']
        start = impl['_M_start']
        finish = impl['_M_finish']
        if not _looks_like_heap_ptr(start):
            return
        start_i, finish_i = int(start), int(finish)
        if finish_i < start_i:
            return
        elem_size = start.dereference().type.sizeof
        count = (finish_i - start_i) // elem_size
        if count < 0 or count > max_count:
            return
        for i in range(count):
            yield start[i]
    except Exception:
        return


def _child_names(impl, limit=4):
    """Names of an operator's children, for the label. Empty list if none."""
    names = []
    try:
        for child in _vector_range(impl['children'], max_count=64):
            resolved = _resolve_impl(child)
            if resolved is None:
                names.append('?')
            else:
                c_impl, c_erased, _ = resolved
                names.append(_read_name(c_impl, c_erased) or '?')
            if len(names) >= limit:
                names.append('...')
                break
    except Exception:
        pass
    return names


def _yield_raw_fields(val):
    """Yield every data member of val unchanged (default-like expansion)."""
    try:
        fields = val.type.strip_typedefs().fields()
    except Exception:
        return
    for f in fields:
        try:
            if not f.name:
                continue
            if getattr(f, 'is_base_class', False):
                continue
            yield (f.name, val[f.name])
        except Exception:
            continue


# ---------------------------------------------------------------------------
# 1) Label printers: custom LABEL, default CHILDREN.
# ---------------------------------------------------------------------------

class LogicalOperatorPrinter:
    def __init__(self, val):
        self.val = val

    def to_string(self):
        resolved = _resolve_impl(self.val)
        if resolved is None:
            return None
        impl, erased, concrete = resolved
        name = _read_name(impl, erased)
        if not name:
            return None
        parts = []
        try:
            parts.append(f'id: {int(concrete["id"]["value"])}')
        except Exception:
            pass
        children = _child_names(impl)
        if children:
            parts.append('children: [' + ', '.join(children) + ']')
        return f'{name} ({", ".join(parts)})' if parts else name

    def children(self):
        return _yield_raw_fields(self.val)

    def display_hint(self):
        return None


class LogicalFunctionPrinter:
    def __init__(self, val):
        self.val = val

    def to_string(self):
        resolved = _resolve_impl(self.val)
        if resolved is None:
            return None
        impl, erased, _ = resolved
        return _read_name(impl, erased)

    def children(self):
        return _yield_raw_fields(self.val)

    def display_hint(self):
        return None


class LogicalPlanPrinter:
    def __init__(self, val):
        self.val = val

    def to_string(self):
        label = 'LogicalPlan'
        try:
            qid = int(self.val['queryId']['value'])
        except Exception:
            return label
        parts = [f'queryId: {qid}']
        names = []
        try:
            for root in _vector_range(self.val['rootOperators'], max_count=64):
                resolved = _resolve_impl(root)
                if resolved is None:
                    names.append('?')
                else:
                    impl, erased, _ = resolved
                    names.append(_read_name(impl, erased) or '?')
                if len(names) >= 4:
                    names.append('...')
                    break
        except Exception:
            pass
        if names:
            parts.append('roots: [' + ', '.join(names) + ']')
        return f'{label} ({", ".join(parts)})'

    def children(self):
        return _yield_raw_fields(self.val)

    def display_hint(self):
        return None


# ---------------------------------------------------------------------------
# 2) Shallow-view printers for NES::Debug::PlanView / OperatorView.
#    These structs are produced by the C++ helper NES::Debug::view(...).
#    Render: label string as the node text, children vector as the only rows.
# ---------------------------------------------------------------------------

class OperatorViewPrinter:
    def __init__(self, val):
        self.val = val

    def to_string(self):
        return _read_std_string(self.val['op']) or 'OperatorView'

    def children(self):
        idx = 0
        try:
            for child in _vector_range(self.val['children'], max_count=4096):
                yield (f'[{idx}]', child)
                idx += 1
        except Exception:
            return

    def display_hint(self):
        return None


class PlanViewPrinter:
    def __init__(self, val):
        self.val = val

    def to_string(self):
        return _read_std_string(self.val['plan']) or 'PlanView'

    def children(self):
        idx = 0
        try:
            for root in _vector_range(self.val['roots'], max_count=4096):
                yield (f'[root {idx}]', root)
                idx += 1
        except Exception:
            return

    def display_hint(self):
        return None


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def nes_pretty_printer_lookup(val):
    try:
        # unqualified() drops const/volatile: Type.name is None for a cv-qualified
        # type, so a `const PlanView` (e.g. `const auto pv = ...`) would otherwise
        # never match and print nothing.
        type_name = val.type.strip_typedefs().unqualified().name
    except Exception:
        return None
    if type_name is None:
        return None
    if type_name == 'NES::LogicalPlan':
        return LogicalPlanPrinter(val)
    if type_name == 'NES::Debug::PlanView':
        return PlanViewPrinter(val)
    if type_name == 'NES::Debug::OperatorView':
        return OperatorViewPrinter(val)
    if 'NES::TypedLogicalOperator<' in type_name:
        return LogicalOperatorPrinter(val)
    if 'NES::TypedLogicalFunction<' in type_name:
        return LogicalFunctionPrinter(val)
    return None


def register_nes_printers(obj):
    if obj is None:
        obj = gdb
    obj.pretty_printers = [
        p for p in obj.pretty_printers
        if p is not nes_pretty_printer_lookup
    ]
    obj.pretty_printers.append(nes_pretty_printer_lookup)


register_nes_printers(None)
print('[NES] Plan/operator labels loaded. '
      'Shallow tree: evaluate NES::Debug::view(plan) in the Watches pane.')
