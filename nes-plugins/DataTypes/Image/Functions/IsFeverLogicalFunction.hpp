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

/// `is_fever(ThermalPixel) -> BOOLEAN`. Same nominal type-gating as
/// ToCelsius. Threshold is currently fixed; can be lifted to a constructor
/// parameter once we add a non-default threshold variant.
class IsFeverLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "IS_FEVER";

    explicit IsFeverLogicalFunction(const LogicalFunction& child);

    [[nodiscard]] bool operator==(const IsFeverLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] IsFeverLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] IsFeverLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    LogicalFunction child;

    friend Reflector<IsFeverLogicalFunction>;
};

template <>
struct Reflector<IsFeverLogicalFunction>
{
    Reflected operator()(const IsFeverLogicalFunction& function) const;
};

template <>
struct Unreflector<IsFeverLogicalFunction>
{
    IsFeverLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<IsFeverLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedIsFeverLogicalFunction
{
    std::optional<LogicalFunction> child;
};
}

FMT_OSTREAM(NES::IsFeverLogicalFunction);
