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

#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{
class ValueOrNullLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "value_or_null";

    ValueOrNullLogicalFunction(LogicalFunction predicate, LogicalFunction value);

    [[nodiscard]] bool operator==(const ValueOrNullLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] ValueOrNullLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] ValueOrNullLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    static LogicalFunctionRegistryReturnType createvalue_or_null(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
    LogicalFunction predicate;
    LogicalFunction value;

    friend Reflector<ValueOrNullLogicalFunction>;
};

template <>
struct Reflector<ValueOrNullLogicalFunction>
{
    Reflected operator()(const ValueOrNullLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<ValueOrNullLogicalFunction>
{
    ValueOrNullLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<ValueOrNullLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedValueOrNullLogicalFunction
{
    std::optional<LogicalFunction> predicate;
    std::optional<LogicalFunction> value;
};
}

FMT_OSTREAM(NES::ValueOrNullLogicalFunction);
