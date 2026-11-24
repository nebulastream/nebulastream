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
#include <Functions/OperatorCallLogicalFunction.hpp>

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>
#include <OperatorFunctionRegistry.hpp>

namespace NES
{
OperatorCallLogicalFunction::OperatorCallLogicalFunction(std::string name, std::vector<LogicalFunction> args)
    : OperatorCallLogicalFunction(std::move(name), std::move(args), DataType{})
{
}

OperatorCallLogicalFunction::OperatorCallLogicalFunction(std::string name, std::vector<LogicalFunction> args, DataType type)
    : functionName(std::move(name)), arguments(std::move(args)), inferredType(std::move(type))
{
}

const std::string& OperatorCallLogicalFunction::getFunctionName() const
{
    return functionName;
}

Identifier OperatorCallLogicalFunction::getOutputName() const
{
    const auto definition = OperatorFunctionRegistry::instance().find(functionName);
    PRECONDITION(definition.has_value(), "Unknown operator function '{}'", functionName);
    return definition->resultName;
}

const std::vector<LogicalFunction>& OperatorCallLogicalFunction::getArguments() const
{
    return arguments;
}

std::vector<LogicalFunction> OperatorCallLogicalFunction::getChildren() const
{
    return arguments;
}

DataType OperatorCallLogicalFunction::getDataType() const
{
    return inferredType;
}

std::string_view OperatorCallLogicalFunction::getType() const
{
    return "OperatorCall";
}

std::string OperatorCallLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    std::string result = functionName + "(";
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        if (i)
        {
            result += ", ";
        }
        result += arguments[i].explain(verbosity);
    }
    return result + ")";
}

OperatorCallLogicalFunction OperatorCallLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    return {functionName, children, inferredType};
}

LogicalFunction OperatorCallLogicalFunction::withInferredDataType(TypeInferenceContext& context) const
{
    std::vector<LogicalFunction> inferred;
    inferred.reserve(arguments.size());
    for (const auto& argument : arguments)
    {
        inferred.push_back(context.inferType(argument));
    }
    const auto entry = OperatorFunctionRegistry::instance().find(functionName);
    PRECONDITION(entry && entry->inferResultType, "No type inference registered for {}", functionName);
    auto type = entry->inferResultType(inferred);
    return LogicalFunction{OperatorCallLogicalFunction{functionName, std::move(inferred), std::move(type)}};
}

LogicalOperator OperatorCallLogicalFunction::toLogicalOperator(LogicalOperator child) const
{
    const auto entry = OperatorFunctionRegistry::instance().find(functionName);
    PRECONDITION(entry && entry->createOperator, "No operator factory registered for {}", functionName);
    const auto input = child.getOutputSchema();
    const auto originalChild = child;
    auto result = entry->createOperator(std::move(child), arguments);
    const auto operatorInputs = result.getChildren();
    if (operatorInputs.size() != 1U || operatorInputs.front() != originalChild)
    {
        throw InvalidQuerySyntax("Operator function '{}' must use the supplied unary input", functionName);
    }
    const auto output = result.getOutputSchema();
    if (output.size() != input.size() + 1U)
    {
        throw InvalidQuerySyntax("Operator function '{}' must append exactly one result field", functionName);
    }
    for (const auto& field : input)
    {
        const auto forwarded = output[field.getFullyQualifiedName()];
        if (!forwarded || forwarded->getDataType() != field.getDataType())
        {
            throw InvalidQuerySyntax("Operator function '{}' must preserve input fields", functionName);
        }
    }
    const auto resultField = output[entry->resultName];
    if (input[entry->resultName] || !resultField || resultField->getDataType() != inferredType)
    {
        throw InvalidQuerySyntax("Operator function '{}' must append its declared result field and type", functionName);
    }
    return result;
}

bool OperatorCallLogicalFunction::operator==(const OperatorCallLogicalFunction& rhs) const
{
    if (functionName != rhs.functionName || inferredType != rhs.inferredType || arguments.size() != rhs.arguments.size())
    {
        return false;
    }
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        if (!StructuralFunctionEqual{}(arguments[i], rhs.arguments[i]))
        {
            return false;
        }
    }
    return true;
}

namespace detail
{
struct ReflectedOperatorCallLogicalFunction
{
    std::string name;
    std::vector<LogicalFunction> arguments;
    DataType type;
};
}

Reflected
Reflector<OperatorCallLogicalFunction>::operator()(const OperatorCallLogicalFunction& value, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedOperatorCallLogicalFunction{value.getFunctionName(), value.getArguments(), value.getDataType()});
}

OperatorCallLogicalFunction
Unreflector<OperatorCallLogicalFunction>::operator()(const Reflected& value, const ReflectionContext& context) const
{
    auto state = context.unreflect<detail::ReflectedOperatorCallLogicalFunction>(value);
    if (!OperatorFunctionRegistry::instance().find(state.name))
    {
        throw CannotDeserialize("Unknown operator function '{}'", state.name);
    }
    return {std::move(state.name), std::move(state.arguments), std::move(state.type)};
}
}

size_t std::hash<NES::OperatorCallLogicalFunction>::operator()(const NES::OperatorCallLogicalFunction& function) const noexcept
{
    size_t hash = std::hash<std::string>{}(function.getFunctionName());
    hash = folly::hash::hash_combine(hash, std::hash<NES::DataType>{}(function.getDataType()));
    for (const auto& arg : function.getArguments())
    {
        hash = folly::hash::hash_combine(hash, NES::StructuralFunctionHash{}(arg));
    }
    return hash;
}
