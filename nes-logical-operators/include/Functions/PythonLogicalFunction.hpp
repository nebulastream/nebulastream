/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0
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
#include <Util/ReflectionFwd.hpp>

namespace NES
{
class PythonLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "Python";

    PythonLogicalFunction(
        std::vector<std::string> parameterNames, std::string body, DataType returnType, std::vector<LogicalFunction> arguments);

    [[nodiscard]] bool operator==(const PythonLogicalFunction& rhs) const;
    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] PythonLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;
    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] const std::vector<std::string>& getParameterNames() const;
    [[nodiscard]] const std::string& getBody() const;

private:
    std::vector<std::string> parameterNames;
    std::string body;
    DataType returnType;
    std::vector<LogicalFunction> arguments;

    friend Reflector<PythonLogicalFunction>;
};

namespace detail
{
struct ReflectedPythonLogicalFunction
{
    std::vector<std::string> parameterNames;
    std::string body;
    DataType returnType;
    std::vector<LogicalFunction> arguments;
};
}

template <>
struct Reflector<PythonLogicalFunction>
{
    Reflected operator()(const PythonLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<PythonLogicalFunction>
{
    PythonLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<PythonLogicalFunction>);
}

FMT_OSTREAM(NES::PythonLogicalFunction);
