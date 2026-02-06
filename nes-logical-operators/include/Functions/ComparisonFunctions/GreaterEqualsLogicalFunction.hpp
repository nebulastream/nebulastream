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
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
class GreaterEqualsLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "GreaterEquals";

    GreaterEqualsLogicalFunction(LogicalFunction left, LogicalFunction right);

    [[nodiscard]] bool operator==(const GreaterEqualsLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] GreaterEqualsLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] GreaterEqualsLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    LogicalFunction left, right;
    DataType dataType;

    friend Reflector<GreaterEqualsLogicalFunction>;
};

template <>
struct Reflector<GreaterEqualsLogicalFunction>
{
    Reflected operator()(const GreaterEqualsLogicalFunction& function) const;
};

template <>
struct Unreflector<GreaterEqualsLogicalFunction>
{
    GreaterEqualsLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<GreaterEqualsLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedGreaterEqualsLogicalFunction
{
    std::optional<LogicalFunction> left;
    std::optional<LogicalFunction> right;
};
}

FMT_OSTREAM(NES::GreaterEqualsLogicalFunction);
