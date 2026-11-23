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

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Util/PlanRenderer.hpp>

namespace NES
{
/// This class adapts the table-valued-function semantics of AsyncLogicalOperator as a scalar function.
/// Instance identity (ptr equality) tracks distinct invocations.
class AsyncCallLogicalFunction
{
public:
    AsyncCallLogicalFunction(const std::string& asyncOperatorName, std::vector<LogicalFunction> args);

    [[nodiscard]] std::string getOperatorName() const { return implementation.getType(); }

    [[nodiscard]] const AsyncOperator& getAsyncOperator() const { return implementation; }

    [[nodiscard]] const std::vector<LogicalFunction>& getArguments() const { return arguments; }

    [[nodiscard]] Identifier getOutputName() const { return *implementation.getOutputSchema().begin()->getFullyQualifiedName().begin(); }

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const { return arguments; }

    [[nodiscard]] DataType getDataType() const { return implementation.getOutputSchema().begin()->getDataType(); }

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) Required by the logical-function interface.
    [[nodiscard]] std::string_view getType() const { return "AsyncCall"; }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] AsyncCallLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;
    [[nodiscard]] LogicalFunction withInferredDataType(TypeInferenceContext& context) const;

    [[nodiscard]] bool operator==(const AsyncCallLogicalFunction& rhs) const;

private:
    friend struct Unreflector<AsyncCallLogicalFunction>;
    AsyncCallLogicalFunction(AsyncOperator implementation, std::vector<LogicalFunction> arguments);
    void validateScalar() const;
    AsyncOperator implementation;
    std::vector<LogicalFunction> arguments;
};

namespace detail
{
struct ReflectedAsyncCallLogicalFunction
{
    AsyncOperator implementation;
    std::vector<LogicalFunction> arguments;
    Identifier outputName;
    DataType dataType;
};
}

template <>
struct Reflector<AsyncCallLogicalFunction>
{
    Reflected operator()(const AsyncCallLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<AsyncCallLogicalFunction>
{
    AsyncCallLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<AsyncCallLogicalFunction>);
}

template <>
struct std::hash<NES::AsyncCallLogicalFunction>
{
    size_t operator()(const NES::AsyncCallLogicalFunction& function) const noexcept;
};
