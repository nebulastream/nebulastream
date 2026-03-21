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

#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>

namespace NES
{

class VarSizedToNumericLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "VarSizedToNumeric";

    VarSizedToNumericLogicalFunction(const LogicalFunction& child, const DataType& targetType);

    DataType getDataType() const;

    VarSizedToNumericLogicalFunction withDataType(const DataType& newDataType) const;

    LogicalFunction withInferredDataType(const Schema& schema) const;

    std::vector<LogicalFunction> getChildren() const;

    VarSizedToNumericLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    std::string_view getType() const;

    bool operator==(const VarSizedToNumericLogicalFunction& rhs) const;

    std::string explain(ExplainVerbosity verbosity) const;

    DataType getTargetType() const;

private:
    static bool isSupportedNumericType(DataType::Type type);

    DataType outputType;
    DataType targetType;
    LogicalFunction child;
};

namespace detail
{

struct ReflectedVarSizedToNumericLogicalFunction
{
    LogicalFunction child;
    DataType targetType;
};

}

template <>
struct Reflector<VarSizedToNumericLogicalFunction>
{
    Reflected operator()(const VarSizedToNumericLogicalFunction& function) const;
};

template <>
struct Unreflector<VarSizedToNumericLogicalFunction>
{
    VarSizedToNumericLogicalFunction operator()(const Reflected& reflected) const;
};

}
