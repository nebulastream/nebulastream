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
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Constructs a nominal STRUCT value from N child expressions, one per field
/// in the struct's declared layout. Reachable from SQL via the type-constructor
/// syntax `<StructName>(arg0, arg1, ...)` (e.g. `Reading(celsius, humidity_pct)`)
/// once the parser has resolved `<StructName>` through the DataType registry.
class ConstructStructLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "ConstructStruct";

    ConstructStructLogicalFunction(DataType structType, std::vector<LogicalFunction> children);

    [[nodiscard]] bool operator==(const ConstructStructLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] ConstructStructLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] ConstructStructLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType structType;
    std::vector<LogicalFunction> children;
};

template <>
struct Reflector<ConstructStructLogicalFunction>
{
    Reflected operator()(const ConstructStructLogicalFunction& function) const;
};

template <>
struct Unreflector<ConstructStructLogicalFunction>
{
    ConstructStructLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<ConstructStructLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedConstructStructLogicalFunction
{
    DataType structType;
    std::vector<LogicalFunction> children;
};
}

FMT_OSTREAM(NES::ConstructStructLogicalFunction);
