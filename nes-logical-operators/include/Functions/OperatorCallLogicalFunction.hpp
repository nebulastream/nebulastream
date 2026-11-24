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
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperatorFwd.hpp>

namespace NES
{
class OperatorCallLogicalFunction
{
public:
    OperatorCallLogicalFunction(std::string functionName, std::vector<LogicalFunction> arguments);
    [[nodiscard]] const std::string& getFunctionName() const;
    [[nodiscard]] Identifier getOutputName() const;
    [[nodiscard]] const std::vector<LogicalFunction>& getArguments() const;
    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] OperatorCallLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;
    [[nodiscard]] LogicalFunction withInferredDataType(TypeInferenceContext& context) const;
    [[nodiscard]] LogicalOperator toLogicalOperator(LogicalOperator child) const;
    [[nodiscard]] bool operator==(const OperatorCallLogicalFunction& rhs) const;

private:
    friend struct Unreflector<OperatorCallLogicalFunction>;
    OperatorCallLogicalFunction(std::string functionName, std::vector<LogicalFunction> arguments, DataType inferredType);
    std::string functionName;
    std::vector<LogicalFunction> arguments;
    DataType inferredType;
};

template <>
struct Reflector<OperatorCallLogicalFunction>
{
    Reflected operator()(const OperatorCallLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<OperatorCallLogicalFunction>
{
    OperatorCallLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<OperatorCallLogicalFunction>);
}

template <>
struct std::hash<NES::OperatorCallLogicalFunction>
{
    size_t operator()(const NES::OperatorCallLogicalFunction& function) const noexcept;
};
