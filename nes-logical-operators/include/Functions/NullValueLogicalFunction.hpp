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
*/

#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

/// The SQL NULL, of a given type: unlike a ConstantValueLogicalFunction, it has no value whose text could tell its type,
/// so the type comes with it (e.g. from Calcite, which types every NULL literal by its context) and is always nullable.
class NullValueLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "NullValue";

    explicit NullValueLogicalFunction(const DataType& dataType);

    [[nodiscard]] bool operator==(const NullValueLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] NullValueLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] NullValueLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    static LogicalFunctionRegistryReturnType createNullValue(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
};

template <>
struct Reflector<NullValueLogicalFunction>
{
    Reflected operator()(const NullValueLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<NullValueLogicalFunction>
{
    NullValueLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<NullValueLogicalFunction>);
}

namespace NES::detail
{
struct ReflectedNullValueLogicalFunction
{
    DataType dataType;
};
}

FMT_OSTREAM(NES::NullValueLogicalFunction);
