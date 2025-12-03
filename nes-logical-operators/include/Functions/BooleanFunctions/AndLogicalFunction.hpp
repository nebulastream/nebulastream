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
class AndLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "And";

    AndLogicalFunction(LogicalFunction left, LogicalFunction right);

    [[nodiscard]] bool operator==(const AndLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] AndLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] AndLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    LogicalFunction left, right;

    friend Reflector<AndLogicalFunction>;
};

template <>
struct Reflector<AndLogicalFunction>
{
    Reflected operator()(const AndLogicalFunction& function) const;
};

template <>
struct Unreflector<AndLogicalFunction>
{
    AndLogicalFunction operator()(const Reflected& rfl) const;
};

static_assert(LogicalFunctionConcept<AndLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedAndLogicalFunction
{
    std::optional<LogicalFunction> left;
    std::optional<LogicalFunction> right;
};
}

FMT_OSTREAM(NES::AndLogicalFunction);
