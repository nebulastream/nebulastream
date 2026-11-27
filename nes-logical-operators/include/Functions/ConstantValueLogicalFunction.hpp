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

#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Types/NESType.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

/// Represents a constant scalar value in the logical expression tree, wrapping a `NESValue` whose `NESType`
/// satisfies the `ScalarDatatype` type class.
class ConstantValueLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "ConstantValue";

    explicit ConstantValueLogicalFunction(NESValue value);
    ConstantValueLogicalFunction(DataType dataType, std::string_view constantValueAsString, bool isNull = false);

    [[nodiscard]] static std::expected<ConstantValueLogicalFunction, Exception> tryCreate(NESValue value);
    [[nodiscard]] static ConstantValueLogicalFunction createNull(DataType dataType);

    [[nodiscard]] const NESValue& getValue() const noexcept;
    [[nodiscard]] std::string getConstantValue() const;
    [[nodiscard]] bool isNullValue() const noexcept;

    [[nodiscard]] bool operator==(const ConstantValueLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] ConstantValueLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] ConstantValueLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    static LogicalFunctionRegistryReturnType createConstantValue(LogicalFunctionRegistryArguments arguments);
    [[nodiscard]] static NESFunctor functor()
    {
        return internalLogicalFunctionFunctor<&ConstantValueLogicalFunction::createConstantValue>(NAME);
    }

private:
    NESValue value;
};

template <>
struct Reflector<ConstantValueLogicalFunction>
{
    Reflected operator()(const ConstantValueLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<ConstantValueLogicalFunction>
{
    ConstantValueLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<ConstantValueLogicalFunction>);
}

namespace NES::detail
{
struct ReflectedConstantValueLogicalFunction
{
    std::string value;
    DataType dataType;
    bool isNull = false;
};
}

FMT_OSTREAM(NES::ConstantValueLogicalFunction);
