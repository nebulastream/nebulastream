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


#include <Identifiers/Identifier.hpp>


#include <Functions/LogicalFunction.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <Schema/Field.hpp>
#include <Util/PlanRenderer.hpp>

namespace NES
{

/// NOLINTBEGIN(readability-convert-member-functions-to-static)
class UnboundFieldAccessLogicalFunction
{
public:
    static constexpr std::string_view NAME = "UnboundFieldAccess";

    explicit UnboundFieldAccessLogicalFunction(Identifier fieldName);
    UnboundFieldAccessLogicalFunction(Identifier fieldName, DataType dataType);

    [[nodiscard]] Identifier getFieldName() const;
    [[nodiscard]] LogicalFunction withFieldName(Identifier fieldName) const;


    [[nodiscard]] bool operator==(const UnboundFieldAccessLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] static std::vector<LogicalFunction> getChildren();
    [[nodiscard]] UnboundFieldAccessLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] static std::string_view getType();
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;


private:
    /// For now its unqualified, but when we have aliased relations, this would become an identifier list
    Identifier fieldName;
    DataType dataType;
    friend std::hash<UnboundFieldAccessLogicalFunction>;
};

/// NOLINTEND(readability-convert-member-functions-to-static)

static_assert(LogicalFunctionConcept<UnboundFieldAccessLogicalFunction>);

template <>
struct Reflector<UnboundFieldAccessLogicalFunction>
{
    Reflected operator()(const UnboundFieldAccessLogicalFunction& function) const;
};

template <>
struct Unreflector<UnboundFieldAccessLogicalFunction>
{
    UnboundFieldAccessLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

}

template <>
struct std::hash<NES::UnboundFieldAccessLogicalFunction>
{
    std::size_t operator()(const NES::UnboundFieldAccessLogicalFunction& function) const noexcept;
};
