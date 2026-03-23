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
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

class VarSizedToNumericLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "VarSizedToNumeric";

    VarSizedToNumericLogicalFunction(const LogicalFunction& child, const DataType& targetType);

    [[nodiscard]] bool operator==(const VarSizedToNumericLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] VarSizedToNumericLogicalFunction withDataType(const DataType& newDataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] VarSizedToNumericLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    [[nodiscard]] DataType getTargetType() const;

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
    VarSizedToNumericLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<VarSizedToNumericLogicalFunction>);

}

FMT_OSTREAM(NES::VarSizedToNumericLogicalFunction);
