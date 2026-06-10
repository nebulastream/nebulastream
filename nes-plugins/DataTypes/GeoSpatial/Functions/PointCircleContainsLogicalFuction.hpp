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

#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Checks if the rhs circle contains the lhs point
class PointCircleContainsLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "POINT_CIRCLE_CONTAINS";

    explicit PointCircleContainsLogicalFunction(const LogicalFunction& leftChild, const LogicalFunction& rightChild);

    [[nodiscard]] bool operator==(const PointCircleContainsLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] PointCircleContainsLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] PointCircleContainsLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    LogicalFunction leftChild;
    LogicalFunction rightChild;

    friend Reflector<PointCircleContainsLogicalFunction>;
};

template <>
struct Reflector<PointCircleContainsLogicalFunction>
{
    Reflected operator()(const PointCircleContainsLogicalFunction& function) const;
};

template <>
struct Unreflector<PointCircleContainsLogicalFunction>
{
    PointCircleContainsLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<PointCircleContainsLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedPointCircleContainsLogicalFunction
{
    std::optional<LogicalFunction> left;
    std::optional<LogicalFunction> right;
};
}

FMT_OSTREAM(NES::PointCircleContainsLogicalFunction);
