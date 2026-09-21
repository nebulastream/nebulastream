/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0
*/
#include <Functions/PythonLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
bool isSupportedType(const DataType::Type type)
{
    return type != DataType::Type::UNDEFINED && type != DataType::Type::CHAR;
}
}

PythonUdfBackend parsePythonUdfBackend(const std::string_view bridge)
{
    if (bridge == "codon" || bridge == "CODON")
    {
        return PythonUdfBackend::Codon;
    }
    if (bridge == "cpython" || bridge == "CPYTHON")
    {
        return PythonUdfBackend::CPython;
    }
    if (bridge == "pypy" || bridge == "PYPY")
    {
        return PythonUdfBackend::PyPy;
    }
    throw UnsupportedUdfLanguage("'{}' is not a valid Python UDF BRIDGE (expected codon, cpython, or pypy)", bridge);
}

PythonLogicalFunction::PythonLogicalFunction(
    std::vector<std::string> parameterNames,
    std::string body,
    DataType returnType,
    std::vector<LogicalFunction> arguments,
    PythonUdfBackend backend)
    : parameterNames(std::move(parameterNames))
    , body(std::move(body))
    , returnType(returnType)
    , arguments(std::move(arguments))
    , backend(backend)
{
    PRECONDITION(this->parameterNames.size() == this->arguments.size(), "Python UDF parameter and argument counts differ");
}

bool PythonLogicalFunction::operator==(const PythonLogicalFunction& rhs) const
{
    return parameterNames == rhs.parameterNames && body == rhs.body && returnType == rhs.returnType && arguments == rhs.arguments
        && backend == rhs.backend;
}

DataType PythonLogicalFunction::getDataType() const
{
    return returnType;
}

LogicalFunction PythonLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.arguments = arguments | std::views::transform([&](const auto& argument) { return argument.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    if (!isSupportedType(copy.returnType.type))
    {
        throw CannotInferStamp("Python UDF has unsupported return type {}", copy.returnType);
    }
    for (const auto& argument : copy.arguments)
    {
        if (!isSupportedType(argument.getDataType().type))
        {
            throw CannotInferStamp("Python UDF has unsupported argument type {}", argument.getDataType());
        }
    }
    return copy;
}

std::vector<LogicalFunction> PythonLogicalFunction::getChildren() const
{
    return arguments;
}

PythonLogicalFunction PythonLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == parameterNames.size(), "Python UDF child count differs from parameter count");
    auto copy = *this;
    copy.arguments = children;
    return copy;
}

std::string_view PythonLogicalFunction::getType() const
{
    return NAME;
}

namespace
{
std::string_view bridgeName(const PythonUdfBackend backend)
{
    switch (backend)
    {
        case PythonUdfBackend::Codon:
            return "codon";
        case PythonUdfBackend::CPython:
            return "cpython";
        case PythonUdfBackend::PyPy:
            return "pypy";
    }
    std::unreachable();
}
}

std::string PythonLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("PythonLogicalFunction({} -> {}, BRIDGE {})", fmt::join(parameterNames, ", "), returnType, bridgeName(backend));
    }
    return fmt::format(
        "PYTHON(({}): $python${}$python$) AS {} BRIDGE '{}'", fmt::join(parameterNames, ", "), body, returnType, bridgeName(backend));
}

const std::vector<std::string>& PythonLogicalFunction::getParameterNames() const
{
    return parameterNames;
}

const std::string& PythonLogicalFunction::getBody() const
{
    return body;
}

PythonUdfBackend PythonLogicalFunction::getBackend() const
{
    return backend;
}

Reflected Reflector<PythonLogicalFunction>::operator()(const PythonLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedPythonLogicalFunction{
        .parameterNames = function.parameterNames,
        .body = function.body,
        .returnType = function.returnType,
        .arguments = function.arguments,
        .backend = function.backend});
}

PythonLogicalFunction Unreflector<PythonLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [parameterNames, body, returnType, arguments, backend] = context.unreflect<detail::ReflectedPythonLogicalFunction>(reflected);
    return PythonLogicalFunction(std::move(parameterNames), std::move(body), returnType, std::move(arguments), backend);
}
}
