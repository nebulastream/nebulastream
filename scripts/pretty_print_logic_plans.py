# NES Query Plan Pretty Printers for GDB
#
# Renders LogicalPlan, LogicalOperator and LogicalFunction variables as a
# collapsible tree in the CLion Variables/Watches pane.
#
# IMPORTANT: This script reads the object graph directly from memory.
# It never calls any C++ function and never copies anything, so it cannot
# crash the inferior — even on partially constructed objects.
#
# Phase 2: per node, show
#   - operator name + id (the node label)
#   - operator-specific simple fields (std::string values)
#   - the predicate function tree (for Selection etc.), recursively
#   - child operators, recursively
# Complex containers (traitSet, schema, descriptors) are skipped for now.
#
# Setup: loaded automatically via the project .gdbinit. No setup needed.

import gdb


# ---------------------------------------------------------------------------
# Low-level safe reads
# ---------------------------------------------------------------------------

def _looks_like_heap_ptr(addr):
    try:
        return int(addr) >= 0x10000
    except Exception:
        return False


def _name_from_type(erased):
    """
    NES::detail::OperatorModel<NES::SourceNameLogicalOperator> -> SourceNameLogicalOperator
    NES::detail::FunctionModel<NES::FieldAccessLogicalFunction> -> FieldAccessLogicalFunction
    """
    try:
        tname = str(erased.dynamic_type)
        inner = tname[tname.index('<') + 1: tname.rindex('>')]
        return inner.split('::')[-1].strip()
    except Exception:
        return '?'


def _read_std_string(val):
    """Read a libstdc++ std::string gdb.Value into a python str, safely."""
    try:
        dataplus = val['_M_dataplus']
        p = dataplus['_M_p']
        if not _looks_like_heap_ptr(p):
            return None
        length = int(val['_M_string_length'])
        if length < 0 or length > 4096:
            return None
        return p.string(length=length)
    except Exception:
        return None


def _is_std_string_type(t):
    try:
        return 'basic_string' in str(t.strip_typedefs())
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Resolving an erased handle (operator OR function) to its concrete impl
# ---------------------------------------------------------------------------

def _resolve_impl(handle_val):
    """
    Given a TypedLogicalOperator or TypedLogicalFunction handle, follow the
    shared_ptr 'self' to the concrete model and return (impl, erased, concrete)
    or None if not safely readable.
    """
    try:
        shared = handle_val['self']
        ptr = shared['_M_ptr']
        if not _looks_like_heap_ptr(ptr):
            return None
        erased = ptr.dereference()
        concrete = erased.cast(erased.dynamic_type)
        impl = concrete['impl']
        return (impl, erased, concrete)
    except Exception:
        return None


def _operator_label(handle_val):
    """Return 'Name (id: N)' label for an operator handle, or None."""
    resolved = _resolve_impl(handle_val)
    if resolved is None:
        return None
    impl, erased, concrete = resolved

    name = None
    try:
        name_sv = impl['NAME']
        length = int(name_sv['_M_len'])
        data = name_sv['_M_str']
        if _looks_like_heap_ptr(data) and 0 < length < 256:
            name = data.string(length=length)
    except Exception:
        pass
    if not name:
        name = _name_from_type(erased)

    op_id = None
    try:
        op_id = int(concrete['id']['value'])
    except Exception:
        pass

    if op_id is not None:
        return f'{name} (id: {op_id})'
    return name


def _function_label(handle_val):
    """Return a label for a LogicalFunction handle, or None."""
    resolved = _resolve_impl(handle_val)
    if resolved is None:
        return None
    impl, erased, concrete = resolved

    name = None
    try:
        name_sv = impl['NAME']
        length = int(name_sv['_M_len'])
        data = name_sv['_M_str']
        if _looks_like_heap_ptr(data) and 0 < length < 256:
            name = data.string(length=length)
    except Exception:
        pass
    if not name:
        name = _name_from_type(erased)
    return name


def _iter_handle_vector(vec_val):
    """Yield each element gdb.Value from a vector of handles (no copies)."""
    if vec_val is None:
        return
    try:
        impl = vec_val['_M_impl']
        start = impl['_M_start']
        finish = impl['_M_finish']
        if not _looks_like_heap_ptr(start):
            return
        start_i = int(start)
        finish_i = int(finish)
        if finish_i < start_i:
            return
        elem_size = start.dereference().type.sizeof
        count = (finish_i - start_i) // elem_size
        if count < 0 or count > 4096:
            return
        for i in range(count):
            yield start[i]
    except Exception:
        return


# Field names we never want to show as "specific fields" because they are
# either handled elsewhere (children) or are complex containers we skip.
_SKIP_FIELDS = {
    'children', 'traitSet', 'schema', 'inputSchema', 'outputSchema',
    'inputSchemas', 'inputOriginIds', 'outputOriginIds',
}


def _iter_simple_fields(impl):
    """
    Yield (field_name, python_str_value) for simple std::string fields on the
    operator impl. Skips complex containers and known structural fields.
    """
    try:
        impl_type = impl.type.strip_typedefs()
    except Exception:
        return
    try:
        fields = impl_type.fields()
    except Exception:
        return
    for f in fields:
        try:
            if not f.name or f.name in _SKIP_FIELDS:
                continue
            ftype = f.type.strip_typedefs()
            if _is_std_string_type(ftype):
                s = _read_std_string(impl[f.name])
                if s is not None:
                    yield (f.name, f'"{s}"')
        except Exception:
            continue


# ---------------------------------------------------------------------------
# Pretty printers
# ---------------------------------------------------------------------------

class LogicalFunctionPrinter:
    """Renders a NES::LogicalFunction (predicate expression) as a tree node."""

    def __init__(self, val):
        self.val = val

    def to_string(self):
        label = _function_label(self.val)
        if label is None:
            return '<LogicalFunction — not readable>'
        return label

    def children(self):
        resolved = _resolve_impl(self.val)
        if resolved is None:
            return
        impl, _, _ = resolved
        # simple string fields of the function (e.g. field name, constant value)
        for fname, fval in _iter_simple_fields(impl):
            yield (fname, fval)
        # nested function children
        try:
            child_vec = impl['children']
        except Exception:
            child_vec = None
        idx = 0
        for child in _iter_handle_vector(child_vec):
            yield (f'[{idx}]', child)
            idx += 1

    def display_hint(self):
        return None


class LogicalOperatorPrinter:
    """Renders a NES::LogicalOperator as a collapsible node."""

    def __init__(self, val):
        self.val = val

    def to_string(self):
        label = _operator_label(self.val)
        if label is None:
            return '<LogicalOperator — not readable>'
        return label

    def children(self):
        resolved = _resolve_impl(self.val)
        if resolved is None:
            return
        impl, _, _ = resolved

        # 1) operator-specific simple string fields (sinkName, logicalSourceName, ...)
        for fname, fval in _iter_simple_fields(impl):
            yield (fname, fval)

        # 2) predicate function, if this operator has one (e.g. Selection)
        try:
            predicate = impl['predicate']
            yield ('predicate', predicate)
        except Exception:
            pass

        # 3) child operators
        try:
            child_vec = impl['children']
        except Exception:
            child_vec = None
        idx = 0
        for child in _iter_handle_vector(child_vec):
            yield (f'[{idx}]', child)
            idx += 1

    def display_hint(self):
        return None


class LogicalPlanPrinter:
    """Renders a NES::LogicalPlan as a collapsible set of root operators."""

    def __init__(self, val):
        self.val = val

    def _roots(self):
        try:
            vec = self.val['rootOperators']
            impl = vec['_M_impl']
            start = int(impl['_M_start'])
            finish = int(impl['_M_finish'])
            cap = int(impl['_M_end_of_storage'])
            if not (0 <= start <= finish <= cap):
                return None
            return vec
        except Exception:
            return None

    def to_string(self):
        if self._roots() is None:
            return '<LogicalPlan — not yet constructed>'
        # Show the query id in the label if available
        try:
            qid = int(self.val['queryId']['value'])
            return f'LogicalPlan (queryId: {qid})'
        except Exception:
            return 'LogicalPlan'

    def children(self):
        roots = self._roots()
        idx = 0
        for root in _iter_handle_vector(roots):
            yield (f'[root {idx}]', root)
            idx += 1

    def display_hint(self):
        return None


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def nes_pretty_printer_lookup(val):
    try:
        type_name = val.type.strip_typedefs().name
    except Exception:
        return None

    if type_name is None:
        return None

    if type_name == 'NES::LogicalPlan':
        return LogicalPlanPrinter(val)

    if 'NES::TypedLogicalOperator' in type_name:
        return LogicalOperatorPrinter(val)

    if 'NES::TypedLogicalFunction' in type_name:
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
print('[NES] Query plan pretty printers loaded (phase 2: fields + predicate).')