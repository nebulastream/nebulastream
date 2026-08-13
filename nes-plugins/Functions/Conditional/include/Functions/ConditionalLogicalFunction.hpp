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
#include <Util/ReflectionFwd.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

/// A single WHEN condition together with its THEN result.
struct WhenThenLogicalFunction
{
    LogicalFunction when;
    LogicalFunction then;

    [[nodiscard]] bool operator==(const WhenThenLogicalFunction& rhs) const = default;
};

/// Represents a CASE WHEN expression: at least one (when, then) pair followed by the default result
/// that applies when no condition holds. The generic child list interface flattens the pairs into
/// [condition1, result1, ..., conditionN, resultN, default] and regroups them on the way back.
class ConditionalLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "Conditional";

    ConditionalLogicalFunction(std::vector<WhenThenLogicalFunction> whenThens, LogicalFunction elseCase);
    explicit ConditionalLogicalFunction(const std::vector<LogicalFunction>& children);

    [[nodiscard]] bool operator==(const ConditionalLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] ConditionalLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    static LogicalFunctionRegistryReturnType createConditional(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
    std::vector<WhenThenLogicalFunction> whenThens;
    LogicalFunction elseCase;
};

namespace detail
{
struct ReflectedConditionalLogicalFunction
{
    std::vector<LogicalFunction> children;
};
}

template <>
struct Unreflector<ConditionalLogicalFunction>
{
    ConditionalLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<ConditionalLogicalFunction>
{
    Reflected operator()(const ConditionalLogicalFunction& function, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<ConditionalLogicalFunction>);

}

FMT_OSTREAM(NES::ConditionalLogicalFunction);
