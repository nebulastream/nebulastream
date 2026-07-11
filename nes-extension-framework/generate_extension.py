#!/usr/bin/env python3
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

"""
generate_extension.py — deterministic NebulaStream extension boilerplate generator.

Reads a .extension.md description file and writes all placeholder C++ files and a
system test into the correct locations under nes-plugins/ and nes-systests/.

Usage:
    python3 nes-extension-framework/generate_extension.py <description.extension.md>
    python3 nes-extension-framework/generate_extension.py <description.extension.md> --dry-run
    python3 nes-extension-framework/generate_extension.py <description.extension.md> --force

Options:
    --dry-run   Show what would be generated without writing any files.
    --force     Overwrite files that already exist (default: skip and warn).
"""

import argparse
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    sys.exit("Error: PyYAML not found. Install with: pip install pyyaml")

# ─── constants ────────────────────────────────────────────────────────────────

_CPP_LICENSE = """\
/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/"""

_CMAKE_LICENSE = """\
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
# limitations under the License."""

_FRONTMATTER_RE = re.compile(r'^---\s*\n(.*?)\n---\s*\n', re.DOTALL)
_SECTION_RE = re.compile(r'^## (.+?)\n(.*?)(?=^## |\Z)', re.DOTALL | re.MULTILINE)
_PARAM_RE = re.compile(
    r'-\s+(\w+)\s+\((\w+),\s*(required|optional)(?:,\s*default=([^)]*))?\)\s*:\s*(.*)'
)

_CPP_TYPE_MAP = {
    'string': 'std::string',
    'int': 'int',
    'bool': 'bool',
    'size': 'size_t',
    'float': 'float',
}

# ─── parsing ──────────────────────────────────────────────────────────────────

def _parse_description(path):
    text = Path(path).read_text()
    m = _FRONTMATTER_RE.match(text)
    if not m:
        sys.exit(f"Error: no YAML frontmatter found in {path}")
    frontmatter = yaml.safe_load(m.group(1)) or {}
    body = text[m.end():]
    sections = {s[0].strip(): s[1].strip() for s in _SECTION_RE.findall(body)}
    return frontmatter, sections


def _validate(frontmatter, path):
    for field in ('type', 'name'):
        if field not in frontmatter:
            sys.exit(f"Error: '{field}' is required in the frontmatter of {path}")
    allowed = ('function', 'operator', 'source', 'sink')
    if frontmatter['type'] not in allowed:
        sys.exit(f"Error: 'type' must be one of: {', '.join(allowed)}")


def _parse_config_params(sections):
    params = []
    for line in sections.get('Config Parameters', '').splitlines():
        m = _PARAM_RE.match(line.strip())
        if m:
            pname, ptype, req, default, desc = m.groups()
            params.append({
                'name': pname,
                'cpp_type': _CPP_TYPE_MAP.get(ptype, 'std::string'),
                'required': req == 'required',
                'default': default or '',
                'desc': desc.strip(),
            })
    return params

# ─── helpers ──────────────────────────────────────────────────────────────────

def _category_to_dir(category):
    """ArithmeticalFunctions → arithmetical"""
    d = category
    if d.endswith('Functions'):
        d = d[:-len('Functions')]
    return d.lower()


def _behavior_text(sections):
    return sections.get('Behavior', 'TODO: implement this method').strip()


def _examples_as_comments(sections, indent='    '):
    lines = sections.get('Examples', '').strip().splitlines()
    return '\n'.join(f'{indent}// {ln}' for ln in lines if ln.strip())


def _write_file(dest, content, dry_run, force, generated):
    dest = Path(dest)
    if dest.exists() and not force:
        print(f"  ⚠  Skipping {dest} (already exists; use --force to overwrite)")
        return
    if not dry_run:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(content)
    generated.append(str(dest))


def _print_report(name, ext_type, generated, next_step, dry_run):
    verb = "Would generate" if dry_run else "Generated"
    print(f"\n{verb} {len(generated)} file(s) for {name} ({ext_type}):")
    for f in generated:
        print(f"  ✓ {f}")
    print(f"\nNext step — add to nes-plugins/CMakeLists.txt:")
    print(f"  {next_step}")
    print()

# ─── config parameter helpers ─────────────────────────────────────────────────

def _param_field(p, descriptor_class):
    if p['required']:
        default_val = 'std::nullopt'
    else:
        t = p['cpp_type']
        dv = p['default']
        if t == 'std::string':
            default_val = f'std::optional<std::string>{{"{dv}"}}'
        elif t == 'bool':
            default_val = f'std::optional<bool>{{{dv or "false"}}}'
        else:
            default_val = f'std::optional<{t}>{{{dv or "0"}}}'
    return (
        f'    static inline const DescriptorConfig::ConfigParameter<{p["cpp_type"]}> {p["name"]}{{\n'
        f'        "{p["name"]}",\n'
        f'        {default_val},\n'
        f'        [](const std::unordered_map<std::string, std::string>& config) {{\n'
        f'            return DescriptorConfig::tryGet({p["name"]}, config);\n'
        f'        }}}};\n'
    )


def _params_map(struct_name, descriptor_class, params):
    extras = ''.join(f', {p["name"]}' for p in params)
    return (
        f'    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap =\n'
        f'        DescriptorConfig::createConfigParameterContainerMap({descriptor_class}::parameterMap{extras});\n'
    )


def _params_struct(struct_name, descriptor_class, params):
    if not params:
        return f'struct {struct_name} {{\n    // no config parameters\n}};\n'
    fields = ''.join(_param_field(p, descriptor_class) for p in params)
    pmap = _params_map(struct_name, descriptor_class, params)
    return f'struct {struct_name}\n{{\n{fields}\n{pmap}}};\n'

# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTION GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def _fn_logical_hpp(name, sf):
    return f"""\
{_CPP_LICENSE}

// Generated from: {sf}

#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{{
class {name}LogicalFunction final
{{
public:
    static constexpr std::string_view NAME = "{name}";

    explicit {name}LogicalFunction(LogicalFunction child);

    [[nodiscard]] bool operator==(const {name}LogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] {name}LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    LogicalFunction child;

    friend Reflector<{name}LogicalFunction>;
}};

template <>
struct Reflector<{name}LogicalFunction>
{{
    Reflected operator()(const {name}LogicalFunction& function, const ReflectionContext& context) const;
}};

template <>
struct Unreflector<{name}LogicalFunction>
{{
    {name}LogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
}};

static_assert(LogicalFunctionConcept<{name}LogicalFunction>);

}}

namespace NES::detail
{{
struct Reflected{name}LogicalFunction
{{
    LogicalFunction child;
}};
}}

FMT_OSTREAM(NES::{name}LogicalFunction);
"""


def _fn_logical_cpp(name, sf, behavior, examples):
    nu = name.upper()
    nl = name.lower()
    return f"""\
{_CPP_LICENSE}

// Generated from: {sf}

#include <{name}LogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{{

{name}LogicalFunction::{name}LogicalFunction(LogicalFunction child) : child(std::move(child))
{{
}}

DataType {name}LogicalFunction::getDataType() const
{{
    return dataType;
}};

LogicalFunction {name}LogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{{
    // TODO: {behavior}
{examples}
    {name}LogicalFunction copy = *this;
    copy.child = child.withInferredDataType(schema);
    if (!copy.child.getDataType().isNumeric())
    {{
        throw CannotInferStamp("Cannot apply {nl} function on non-numeric input function {{}}", copy.child);
    }}
    copy.dataType = copy.child.getDataType();
    return copy;
}};

std::vector<LogicalFunction> {name}LogicalFunction::getChildren() const
{{
    return {{child}};
}};

{name}LogicalFunction {name}LogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{{
    PRECONDITION(children.size() == 1, "{name}LogicalFunction requires exactly one child, but got {{}}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}};

std::string_view {name}LogicalFunction::getType() const
{{
    return NAME;
}}

bool {name}LogicalFunction::operator==(const {name}LogicalFunction& rhs) const
{{
    return child == rhs.child;
}}

std::string {name}LogicalFunction::explain(ExplainVerbosity verbosity) const
{{
    if (verbosity == ExplainVerbosity::Debug)
    {{
        return fmt::format("{name}LogicalFunction({{}} : {{}})", child.explain(verbosity), dataType);
    }}
    return fmt::format("{nu}({{}})", child.explain(verbosity));
}}

Reflected Reflector<{name}LogicalFunction>::operator()(const {name}LogicalFunction& function, const ReflectionContext& context) const
{{
    return context.reflect(detail::Reflected{name}LogicalFunction{{.child = function.child}});
}}

{name}LogicalFunction Unreflector<{name}LogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{{
    auto [child] = context.unreflect<detail::Reflected{name}LogicalFunction>(reflected);
    return {name}LogicalFunction(std::move(child));
}}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::Register{name}LogicalFunction(LogicalFunctionRegistryArguments arguments)
{{
    if (arguments.children.size() != 1)
    {{
        throw CannotDeserialize("{name}LogicalFunction requires exactly one child, but got {{}}", arguments.children.size());
    }}
    return {name}LogicalFunction(arguments.children[0]);
}}

}}
"""


def _fn_physical_hpp(name, sf):
    return f"""\
{_CPP_LICENSE}

// Generated from: {sf}

#pragma once

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>

namespace NES
{{

class {name}PhysicalFunction final
{{
public:
    explicit {name}PhysicalFunction(PhysicalFunction childFunction, DataType inputType, DataType outputType);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    PhysicalFunction childFunction;
    DataType inputType;
    DataType outputType;
}};

static_assert(PhysicalFunctionConcept<{name}PhysicalFunction>);

}}
"""


def _fn_physical_cpp(name, sf, behavior, examples):
    return f"""\
{_CPP_LICENSE}

// Generated from: {sf}

#include <{name}PhysicalFunction.hpp>

#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{{

{name}PhysicalFunction::{name}PhysicalFunction(PhysicalFunction childFunction, DataType inputType, DataType outputType)
    : childFunction(std::move(childFunction)), inputType(std::move(inputType)), outputType(std::move(outputType))
{{
}}

VarVal {name}PhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{{
    // TODO: {behavior}
{examples}
    auto value = childFunction.execute(record, arena);
    return value.castToType(outputType.type);  // TODO: replace with actual computation
}}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::Register{name}PhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1, "{name} function must have exactly one child function");
    PRECONDITION(physicalFunctionRegistryArguments.inputTypes.size() == 1, "{name} function must have exactly one input type");
    return {name}PhysicalFunction(
        physicalFunctionRegistryArguments.childFunctions[0],
        physicalFunctionRegistryArguments.inputTypes[0],
        physicalFunctionRegistryArguments.outputType);
}}

}}
"""


def _fn_cmake(name, sf):
    lp = name.lower()
    return f"""\
{_CMAKE_LICENSE}

# Generated from: {sf}

include(${{PROJECT_SOURCE_DIR}}/cmake/PluginRegistrationUtil.cmake)
include(${{PROJECT_SOURCE_DIR}}/cmake/UnreflectionRegistrationUtil.cmake)

add_plugin_as_library({name} LogicalFunction {lp}_logical_lib {name}LogicalFunction.cpp)
target_include_directories({lp}_logical_lib PRIVATE ${{CMAKE_CURRENT_SOURCE_DIR}})

add_plugin_as_library({name} PhysicalFunction {lp}_physical_lib {name}PhysicalFunction.cpp)
target_include_directories({lp}_physical_lib PRIVATE ${{CMAKE_CURRENT_SOURCE_DIR}})

# add_unreflection_plugin computes the include path relative to a src/ ancestor, which does
# not exist under nes-plugins/. We expose the plugin directory to the glue library so that
# the generated unreflector TU can #include <{name}LogicalFunction.hpp> successfully.
get_property(_unr_lib GLOBAL PROPERTY "LogicalFunction_UNREFLECTION_GLUE_LIB")
target_include_directories(${{_unr_lib}} PRIVATE ${{CMAKE_CURRENT_SOURCE_DIR}})
add_unreflection_plugin(LogicalFunction {name})
"""


def _fn_systest(name, cat_dir, sections):
    nu = name.upper()
    return f"""\
# name: function/{cat_dir}/{name}Function.test
# description: Test {name} function
# groups: [Function, Function{name}]

CREATE LOGICAL SOURCE stream(x FLOAT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR stream TYPE File;
ATTACH INLINE
0.0
1.0
-1.0

CREATE SINK result(y FLOAT64 NOT NULL) TYPE File;

SELECT {nu}(x) AS y FROM stream INTO result;
----
# TODO: Replace with actual expected output values
# {nu}(0.0) = ?
# {nu}(1.0) = ?
# {nu}(-1.0) = ?
"""


def generate_function(name, sf, sections, category, dry_run, force, root):
    cat_dir = _category_to_dir(category)
    behavior = _behavior_text(sections)
    examples = _examples_as_comments(sections)

    plugin_dir = root / f'nes-plugins/Functions/{name}'
    systest_dir = root / f'nes-systests/function/{cat_dir}'

    generated = []
    _write_file(plugin_dir / f'{name}LogicalFunction.hpp',
                _fn_logical_hpp(name, sf), dry_run, force, generated)
    _write_file(plugin_dir / f'{name}LogicalFunction.cpp',
                _fn_logical_cpp(name, sf, behavior, examples), dry_run, force, generated)
    _write_file(plugin_dir / f'{name}PhysicalFunction.hpp',
                _fn_physical_hpp(name, sf), dry_run, force, generated)
    _write_file(plugin_dir / f'{name}PhysicalFunction.cpp',
                _fn_physical_cpp(name, sf, behavior, examples), dry_run, force, generated)
    _write_file(plugin_dir / 'CMakeLists.txt',
                _fn_cmake(name, sf), dry_run, force, generated)
    _write_file(systest_dir / f'{name}Function.test',
                _fn_systest(name, cat_dir, sections), dry_run, force, generated)

    return generated, f'activate_optional_plugin("Functions/{name}" ON)'

# ═══════════════════════════════════════════════════════════════════════════════
# OPERATOR GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def _op_logical_hpp(name, sf):
    return f"""\
{_CPP_LICENSE}

// Generated from: {sf}

#pragma once

#include <optional>
#include <string_view>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ManagedByOperator.hpp>
#include <Schema/Field.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{{
class {name}LogicalOperator : public ManagedByOperator
{{
public:
    explicit {name}LogicalOperator(WeakLogicalOperator self);
    {name}LogicalOperator(WeakLogicalOperator self, LogicalOperator child);

    static TypedLogicalOperator<{name}LogicalOperator> create();

    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] {name}LogicalOperator withInferredSchema() const;

    [[nodiscard]] TraitSet getTraitSet() const;
    [[nodiscard]] {name}LogicalOperator withTraitSet(TraitSet traitSet) const;

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] {name}LogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] {name}LogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] bool operator==(const {name}LogicalOperator& rhs) const;

private:
    static constexpr std::string_view NAME = "{name}";
    std::optional<LogicalOperator> child;
    TraitSet traitSet;

    friend Reflector<{name}LogicalOperator>;
}};

template <>
struct Reflector<{name}LogicalOperator>
{{
    Reflected operator()(const {name}LogicalOperator& op, const ReflectionContext& context) const;
}};

template <>
struct Unreflector<{name}LogicalOperator>
{{
    {name}LogicalOperator operator()(const Reflected& reflected, const ReflectionContext& context) const;
}};

static_assert(LogicalOperatorConcept<{name}LogicalOperator>);

}}

namespace NES::detail
{{
struct Reflected{name}LogicalOperator
{{
    std::optional<LogicalOperator> child;
    TraitSet traitSet;
}};
}}
"""


def _op_logical_cpp(name, sf, behavior):
    return f"""\
{_CPP_LICENSE}

// Generated from: {sf}

#include <{name}LogicalOperator.hpp>

#include <utility>
#include <Serialization/LogicalOperatorReflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{{

{name}LogicalOperator::{name}LogicalOperator(WeakLogicalOperator self)
    : ManagedByOperator(self)
{{
}}

{name}LogicalOperator::{name}LogicalOperator(WeakLogicalOperator self, LogicalOperator child)
    : ManagedByOperator(self), child(std::move(child))
{{
}}

TypedLogicalOperator<{name}LogicalOperator> {name}LogicalOperator::create()
{{
    // TODO: {behavior}
    return LogicalOperator::create<{name}LogicalOperator>();
}}

std::string_view {name}LogicalOperator::getName() const noexcept
{{
    return NAME;
}}

Schema<Field, Unordered> {name}LogicalOperator::getOutputSchema() const
{{
    // TODO: compute output schema from child and operator logic
    PRECONDITION(child.has_value(), "{name}LogicalOperator requires a child");
    return child->getOutputSchema();
}}

{name}LogicalOperator {name}LogicalOperator::withInferredSchema() const
{{
    // TODO: propagate schema inference from child
    auto copy = *this;
    if (copy.child)
        copy.child = copy.child->withInferredSchema();
    return copy;
}}

TraitSet {name}LogicalOperator::getTraitSet() const
{{
    return traitSet;
}}

{name}LogicalOperator {name}LogicalOperator::withTraitSet(TraitSet ts) const
{{
    auto copy = *this;
    copy.traitSet = std::move(ts);
    return copy;
}}

std::vector<LogicalOperator> {name}LogicalOperator::getChildren() const
{{
    if (child) return {{*child}};
    return {{}};
}}

{name}LogicalOperator {name}LogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{{
    PRECONDITION(children.size() <= 1, "{name}LogicalOperator requires at most one child");
    auto copy = *this;
    copy.child = children.empty() ? std::nullopt : std::optional{{children[0]}};
    return copy;
}}

{name}LogicalOperator {name}LogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{{
    return withChildren(std::move(children));
}}

std::string {name}LogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{{
    return fmt::format("{name}[{{}}]", id);
}}

bool {name}LogicalOperator::operator==(const {name}LogicalOperator& rhs) const
{{
    return child == rhs.child && traitSet == rhs.traitSet;
}}

Reflected Reflector<{name}LogicalOperator>::operator()(const {name}LogicalOperator& op, const ReflectionContext& context) const
{{
    return context.reflect(detail::Reflected{name}LogicalOperator{{.child = op.child, .traitSet = op.traitSet}});
}}

{name}LogicalOperator Unreflector<{name}LogicalOperator>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{{
    auto [child, traitSet] = context.unreflect<detail::Reflected{name}LogicalOperator>(reflected);
    // TODO: reconstruct with proper arguments
    if (child)
        return {{WeakLogicalOperator{{}}, *child}};
    return {{WeakLogicalOperator{{}}}};
}}

LogicalOperatorRegistryReturnType LogicalOperatorGeneratedRegistrar::Register{name}LogicalOperator(LogicalOperatorRegistryArguments arguments)
{{
    return {name}LogicalOperator::create();
}}

}}
"""


def _op_physical_hpp(name, sf, stateful):
    handler_stub = ''
    if stateful:
        handler_stub = f"""
class {name}OperatorHandler : public OperatorHandler
{{
public:
    // TODO: add state fields (e.g. hash sets, accumulators, buffers)
    void setup(ExecutionContext& ctx) override {{}}
    void terminate(ExecutionContext& ctx) override {{}}
}};
"""
    return f"""\
{_CPP_LICENSE}

// Generated from: {sf}

#pragma once

#include <optional>
#include <Interface/Record.hpp>
#include <PhysicalOperator.hpp>
{handler_stub}
namespace NES
{{

class {name}PhysicalOperator final
{{
public:
    explicit {name}PhysicalOperator();

    void execute(ExecutionContext& ctx, Record& record) const;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const;
    void setChild(PhysicalOperator child);

private:
    std::optional<PhysicalOperator> child;
}};

}}
"""


def _op_physical_cpp(name, sf, behavior):
    return f"""\
{_CPP_LICENSE}

// Generated from: {sf}

#include <{name}PhysicalOperator.hpp>

namespace NES
{{

{name}PhysicalOperator::{name}PhysicalOperator() = default;

void {name}PhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{{
    // TODO: {behavior}
    if (child)
        child->execute(ctx, record);
}}

std::optional<PhysicalOperator> {name}PhysicalOperator::getChild() const
{{
    return child;
}}

void {name}PhysicalOperator::setChild(PhysicalOperator c)
{{
    child = std::move(c);
}}

}}
"""


def _op_lowering_hpp(name, sf):
    return f"""\
{_CPP_LICENSE}

// Generated from: {sf}

#pragma once

#include <LoweringRules/AbstractLoweringRule.hpp>
#include <QueryCompilerConfiguration.hpp>

namespace NES
{{

class LowerToPhysical{name} : public AbstractLoweringRule
{{
public:
    explicit LowerToPhysical{name}(const QueryCompilerConfiguration& conf);
    LoweringRuleResultSubgraph apply(LogicalOperator logicalOperator) override;
}};

}}
"""


def _op_lowering_cpp(name, sf, behavior):
    return f"""\
{_CPP_LICENSE}

// Generated from: {sf}

#include <LowerToPhysical{name}.hpp>
#include <{name}LogicalOperator.hpp>
#include <{name}PhysicalOperator.hpp>
#include <LoweringRuleRegistry.hpp>
#include <Util/SchemaFactory.hpp>
#include <PhysicalOperator.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>

namespace NES
{{

LowerToPhysical{name}::LowerToPhysical{name}(const QueryCompilerConfiguration& conf)
    : AbstractLoweringRule(conf)
{{
}}

LoweringRuleResultSubgraph LowerToPhysical{name}::apply(LogicalOperator logicalOperator)
{{
    // TODO: {behavior}
    const auto traitSet = logicalOperator.getTraitSet();
    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto inputSchema  = createPhysicalOutputSchema(logicalOperator.getChildren()[0].getTraitSet());

    auto physicalOperator = {name}PhysicalOperator();
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        inputSchema, outputSchema,
        memoryLayoutType, memoryLayoutType,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    std::vector leaves(logicalOperator.getChildren().size(), wrapper);
    return {{.root = wrapper, .leaves = {{leaves}}}};
}}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::Register{name}LoweringRule(LoweringRuleRegistryArguments argument)
{{
    return std::make_unique<LowerToPhysical{name}>(argument.conf);
}}

}}
"""


def _op_cmake(name, sf):
    lp = name.lower()
    return f"""\
{_CMAKE_LICENSE}

# Generated from: {sf}

include(${{PROJECT_SOURCE_DIR}}/cmake/PluginRegistrationUtil.cmake)
include(${{PROJECT_SOURCE_DIR}}/cmake/UnreflectionRegistrationUtil.cmake)

# Logical operator + physical operator compiled together (physical has no separate registry)
add_plugin_as_library({name} LogicalOperator {lp}_logical_lib
    {name}LogicalOperator.cpp
    {name}PhysicalOperator.cpp)
target_include_directories({lp}_logical_lib PRIVATE ${{CMAKE_CURRENT_SOURCE_DIR}})

# Lowering rule registered separately
add_plugin_as_library({name} LoweringRule {lp}_lowering_lib LowerToPhysical{name}.cpp)
target_include_directories({lp}_lowering_lib PRIVATE ${{CMAKE_CURRENT_SOURCE_DIR}})

# Unreflection for logical operator plan serialization
get_property(_unr_lib GLOBAL PROPERTY "LogicalOperator_UNREFLECTION_GLUE_LIB")
target_include_directories(${{_unr_lib}} PRIVATE ${{CMAKE_CURRENT_SOURCE_DIR}})
add_unreflection_plugin(LogicalOperator {name})
"""


def _op_systest(name, sections):
    nl = name.lower()
    return f"""\
# name: operator/{nl}/{name}Basic.test
# description: Basic placeholder test for {name} operator
# groups: [{name}]

CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL, value FLOAT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR stream TYPE File;
ATTACH INLINE
1,1.5
2,2.5
1,3.5
3,4.5

CREATE SINK output(id UINT64 NOT NULL, value FLOAT64 NOT NULL) TYPE File;

# TODO: Replace this placeholder SELECT with the actual query using the {name} operator
SELECT stream.id, stream.value FROM stream INTO output;
----
# TODO: Add expected output after the operator is applied
1,1.5
2,2.5
3,4.5
"""


def generate_operator(name, sf, sections, stateful, dry_run, force, root):
    nl = name.lower()
    behavior = _behavior_text(sections)

    plugin_dir = root / f'nes-plugins/Operators/{name}'
    systest_dir = root / f'nes-systests/operator/{nl}'

    generated = []
    _write_file(plugin_dir / f'{name}LogicalOperator.hpp',
                _op_logical_hpp(name, sf), dry_run, force, generated)
    _write_file(plugin_dir / f'{name}LogicalOperator.cpp',
                _op_logical_cpp(name, sf, behavior), dry_run, force, generated)
    _write_file(plugin_dir / f'{name}PhysicalOperator.hpp',
                _op_physical_hpp(name, sf, stateful), dry_run, force, generated)
    _write_file(plugin_dir / f'{name}PhysicalOperator.cpp',
                _op_physical_cpp(name, sf, behavior), dry_run, force, generated)
    _write_file(plugin_dir / f'LowerToPhysical{name}.hpp',
                _op_lowering_hpp(name, sf), dry_run, force, generated)
    _write_file(plugin_dir / f'LowerToPhysical{name}.cpp',
                _op_lowering_cpp(name, sf, behavior), dry_run, force, generated)
    _write_file(plugin_dir / 'CMakeLists.txt',
                _op_cmake(name, sf), dry_run, force, generated)
    _write_file(systest_dir / f'{name}Basic.test',
                _op_systest(name, sections), dry_run, force, generated)

    return generated, f'activate_optional_plugin("Operators/{name}" ON)'

# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def _src_hpp(name, sf, params):
    struct_body = _params_struct(f'ConfigParameters{name}', 'SourceDescriptor', params)
    return f"""\
{_CPP_LICENSE}

// Generated from: {sf}

#pragma once

#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{{

{struct_body}
class {name} final : public Source
{{
public:
    static constexpr std::string_view NAME = "{name}";

    explicit {name}(const SourceDescriptor& sourceDescriptor);
    ~{name}() override = default;

    {name}(const {name}&) = delete;
    {name}& operator=(const {name}&) = delete;
    {name}({name}&&) = delete;
    {name}& operator=({name}&&) = delete;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    // TODO: add state fields (file handles, connections, etc.)
}};

}}
"""


def _src_cpp(name, sf, params, behavior):
    param_getters = ''.join(
        f'    // , {p["name"]}(sourceDescriptor.getFromConfig(ConfigParameters{name}::{p["name"]}))\n'
        for p in params
    )
    return f"""\
{_CPP_LICENSE}

// Generated from: {sf}

#include <{name}.hpp>

#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{{

{name}::{name}(const SourceDescriptor& sourceDescriptor)
    // TODO: initialize fields from config
    // Example: , myParam(sourceDescriptor.getFromConfig(ConfigParameters{name}::MY_PARAM))
{param_getters}{{
}}

void {name}::open(std::shared_ptr<AbstractBufferProvider>)
{{
    // TODO: {behavior}
}}

void {name}::close()
{{
    // TODO: close connections / release resources
}}

Source::FillTupleBufferResult {name}::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{{
    if (stopToken.stop_requested())
        return FillTupleBufferResult::eos();
    // TODO: read data into tupleBuffer.getAvailableMemoryArea<char>()
    // Return FillTupleBufferResult::withBytes(n) or FillTupleBufferResult::eos()
    return FillTupleBufferResult::eos();
}}

DescriptorConfig::Config {name}::validateAndFormat(std::unordered_map<std::string, std::string> config)
{{
    return DescriptorConfig::validateAndFormat<ConfigParameters{name}>(std::move(config), NAME);
}}

std::ostream& {name}::toString(std::ostream& str) const
{{
    return str << "{name}()";
}}

SourceValidationRegistryReturnType RegisterSourceValidation(SourceValidationRegistryArguments args)
{{
    return {name}::validateAndFormat(std::move(args.config));
}}

SourceRegistryReturnType SourceGeneratedRegistrar::Register{name}(SourceRegistryArguments args)
{{
    return std::make_unique<{name}>(args.sourceDescriptor);
}}

}}
"""


def _src_cmake_plugin(name, sf):
    lp = name.lower()
    return f"""\
{_CMAKE_LICENSE}

# Generated from: {sf}

include(${{PROJECT_SOURCE_DIR}}/cmake/PluginRegistrationUtil.cmake)

add_plugin_as_library({name} Source {lp}_source_lib {name}.cpp)
add_plugin_as_library({name} SourceValidation {lp}_source_val_lib {name}.cpp)
target_include_directories({lp}_source_lib PUBLIC ${{CMAKE_CURRENT_SOURCE_DIR}})
target_include_directories({lp}_source_val_lib PUBLIC ${{CMAKE_CURRENT_SOURCE_DIR}})
"""


def _src_systest(name, params, sections):
    nl = name.lower()
    set_block = '\n'.join(
        f"    # TODO: '{p['default'] or 'value'}' AS \"SOURCE\".{p['name']}"
        for p in params
    ) or "    # no config parameters"
    return f"""\
# name: sources/{name}.test
# description: Placeholder test for {name} — TODO: verify records are ingested correctly
# groups: [Sources, {name}]

# TODO: If {name} requires external infrastructure (broker, server, device), describe setup here.

CREATE LOGICAL SOURCE {nl}Input(
    # TODO: Match this schema to the actual data produced by {name}
    id UINT64 NOT NULL,
    value FLOAT64 NOT NULL
);

CREATE PHYSICAL SOURCE FOR {nl}Input TYPE {name}
SET(
{set_block}
);

CREATE SINK output(id UINT64 NOT NULL, value FLOAT64 NOT NULL) TYPE File;

SELECT {nl}Input.id, {nl}Input.value
FROM {nl}Input
INTO output;
----
# TODO: Add expected output rows in CSV format
# 1,1.5
# 2,2.5
"""


def generate_source(name, sf, sections, plugin, dry_run, force, root):
    params = _parse_config_params(sections)
    behavior = _behavior_text(sections)

    generated = []

    if plugin:
        plugin_dir = root / f'nes-plugins/Sources/{name}'
        _write_file(plugin_dir / f'{name}.hpp',
                    _src_hpp(name, sf, params), dry_run, force, generated)
        _write_file(plugin_dir / f'{name}.cpp',
                    _src_cpp(name, sf, params, behavior), dry_run, force, generated)
        _write_file(plugin_dir / 'CMakeLists.txt',
                    _src_cmake_plugin(name, sf), dry_run, force, generated)
        next_step = f'activate_optional_plugin("Sources/{name}" ON)'
    else:
        _write_file(root / f'nes-sources/private/{name}.hpp',
                    _src_hpp(name, sf, params), dry_run, force, generated)
        _write_file(root / f'nes-sources/src/{name}.cpp',
                    _src_cpp(name, sf, params, behavior), dry_run, force, generated)
        next_step = (f'In nes-sources/src/CMakeLists.txt:\n'
                     f'  add_plugin({name} Source {name}.cpp)\n'
                     f'  add_plugin({name} SourceValidation {name}.cpp)')

    _write_file(root / f'nes-systests/sources/{name}.test',
                _src_systest(name, params, sections), dry_run, force, generated)

    return generated, next_step

# ═══════════════════════════════════════════════════════════════════════════════
# SINK GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def _sink_hpp(name, sf, params):
    struct_body = _params_struct(f'ConfigParameters{name}', 'SinkDescriptor', params)
    return f"""\
{_CPP_LICENSE}

// Generated from: {sf}

#pragma once

#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>

namespace NES
{{

{struct_body}
class {name} final : public Sink
{{
public:
    static constexpr std::string_view NAME = "{name}";

    explicit {name}(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~{name}() override = default;

    {name}(const {name}&) = delete;
    {name}& operator=(const {name}&) = delete;
    {name}({name}&&) = delete;
    {name}& operator=({name}&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    // TODO: add state fields
}};

}}
"""


def _sink_cpp(name, sf, params, behavior):
    param_getters = ''.join(
        f'    // , {p["name"]}(sinkDescriptor.getFromConfig(ConfigParameters{name}::{p["name"]}))\n'
        for p in params
    )
    return f"""\
{_CPP_LICENSE}

// Generated from: {sf}

#include <{name}.hpp>

#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{{

{name}::{name}(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    // TODO: initialize fields from config
    // Example: , myParam(sinkDescriptor.getFromConfig(ConfigParameters{name}::MY_PARAM))
{param_getters}{{
}}

void {name}::start(PipelineExecutionContext&)
{{
    // TODO: {behavior}
    NES_DEBUG("Starting {name}");
}}

void {name}::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in {name}");
    // TODO: write/send buffer contents to the destination
}}

void {name}::stop(PipelineExecutionContext&)
{{
    // TODO: flush remaining data and close resources
    NES_INFO("Stopping {name}");
}}

DescriptorConfig::Config {name}::validateAndFormat(std::unordered_map<std::string, std::string> config)
{{
    return DescriptorConfig::validateAndFormat<ConfigParameters{name}>(std::move(config), NAME);
}}

std::ostream& {name}::toString(std::ostream& str) const
{{
    return str << "{name}()";
}}

SinkValidationRegistryReturnType RegisterSinkValidation(SinkValidationRegistryArguments args)
{{
    return {name}::validateAndFormat(std::move(args.config));
}}

SinkRegistryReturnType RegisterSink(SinkRegistryArguments args)
{{
    return std::make_unique<{name}>(std::move(args.backpressureController), args.sinkDescriptor);
}}

}}
"""


def _sink_cmake_plugin(name, sf):
    lp = name.lower()
    return f"""\
{_CMAKE_LICENSE}

# Generated from: {sf}

include(${{PROJECT_SOURCE_DIR}}/cmake/PluginRegistrationUtil.cmake)

add_plugin_as_library({name} Sink {lp}_sink_lib {name}.cpp)
add_plugin_as_library({name} SinkValidation {lp}_sink_val_lib {name}.cpp)
target_include_directories({lp}_sink_lib PUBLIC ${{CMAKE_CURRENT_SOURCE_DIR}})
target_include_directories({lp}_sink_val_lib PUBLIC ${{CMAKE_CURRENT_SOURCE_DIR}})
"""


def _sink_systest(name, params, sections):
    nl = name.lower()
    set_block = '\n'.join(
        f"    # TODO: '{p['default'] or 'value'}' AS \"SINK\".{p['name']}"
        for p in params
    ) or "    # no config parameters"
    return f"""\
# name: sinks/{name}.test
# description: Placeholder test for {name} — TODO: verify records reach the destination
# groups: [Sinks, {name}]

# TODO: If {name} requires external infrastructure, describe setup here.

CREATE LOGICAL SOURCE input(id UINT64 NOT NULL, value FLOAT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
1,1.5
2,2.5
3,3.5

CREATE SINK {nl}Output(id UINT64 NOT NULL, value FLOAT64 NOT NULL) TYPE {name}
SET(
{set_block}
);

SELECT input.id, input.value
FROM input
INTO {nl}Output;
----
# TODO: If {name} writes to a local file, add expected CSV output here.
# If it writes to a remote destination, leave empty and verify externally.
"""


def generate_sink(name, sf, sections, plugin, dry_run, force, root):
    params = _parse_config_params(sections)
    behavior = _behavior_text(sections)

    generated = []

    if plugin:
        plugin_dir = root / f'nes-plugins/Sinks/{name}'
        _write_file(plugin_dir / f'{name}.hpp',
                    _sink_hpp(name, sf, params), dry_run, force, generated)
        _write_file(plugin_dir / f'{name}.cpp',
                    _sink_cpp(name, sf, params, behavior), dry_run, force, generated)
        _write_file(plugin_dir / 'CMakeLists.txt',
                    _sink_cmake_plugin(name, sf), dry_run, force, generated)
        next_step = f'activate_optional_plugin("Sinks/{name}" ON)'
    else:
        _write_file(root / f'nes-sinks/include/Sinks/{name}.hpp',
                    _sink_hpp(name, sf, params), dry_run, force, generated)
        _write_file(root / f'nes-sinks/src/{name}.cpp',
                    _sink_cpp(name, sf, params, behavior), dry_run, force, generated)
        next_step = (f'In nes-sinks/src/CMakeLists.txt:\n'
                     f'  add_plugin({name} Sink {name}.cpp)\n'
                     f'  add_plugin({name} SinkValidation {name}.cpp)')

    _write_file(root / f'nes-systests/sinks/{name}.test',
                _sink_systest(name, params, sections), dry_run, force, generated)

    return generated, next_step

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='Generate NebulaStream extension boilerplate from a .extension.md file.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('description_file', metavar='<description.extension.md>',
                        help='Path to the extension description file')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be generated without writing any files')
    parser.add_argument('--force', action='store_true',
                        help='Overwrite existing files (default: skip and warn)')
    args = parser.parse_args()

    # Locate the project root (directory containing nes-plugins/)
    desc_path = Path(args.description_file).resolve()
    if not desc_path.exists():
        sys.exit(f"Error: file not found: {args.description_file}")

    # Resolve project root: walk up from description file, then fall back to CWD
    root = None
    for candidate in [desc_path.parent] + list(desc_path.parents):
        if (candidate / 'nes-plugins').is_dir():
            root = candidate
            break
    if root is None:
        cwd = Path.cwd()
        for candidate in [cwd] + list(cwd.parents):
            if (candidate / 'nes-plugins').is_dir():
                root = candidate
                break
    if root is None:
        sys.exit("Error: cannot locate project root (no nes-plugins/ directory found). "
                 "Run from the project root or place the description file inside the project.")

    sf = desc_path.name  # basename used in "Generated from:" comments

    frontmatter, sections = _parse_description(str(desc_path))
    _validate(frontmatter, str(desc_path))

    ext_type = frontmatter['type']
    name = frontmatter['name']

    if args.dry_run:
        print(f"[dry-run] Would generate {ext_type} '{name}' in {root}")

    if ext_type == 'function':
        category = frontmatter.get('category', 'ArithmeticalFunctions')
        generated, next_step = generate_function(
            name, sf, sections, category, args.dry_run, args.force, root)

    elif ext_type == 'operator':
        stateful = bool(frontmatter.get('stateful', False))
        generated, next_step = generate_operator(
            name, sf, sections, stateful, args.dry_run, args.force, root)

    elif ext_type == 'source':
        plugin = frontmatter.get('plugin', True)
        generated, next_step = generate_source(
            name, sf, sections, plugin, args.dry_run, args.force, root)

    elif ext_type == 'sink':
        plugin = frontmatter.get('plugin', True)
        generated, next_step = generate_sink(
            name, sf, sections, plugin, args.dry_run, args.force, root)

    _print_report(name, ext_type, generated, next_step, args.dry_run)


if __name__ == '__main__':
    main()
