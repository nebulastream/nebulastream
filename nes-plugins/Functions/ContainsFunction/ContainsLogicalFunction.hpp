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

class ContainsLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "Contains";

    ContainsLogicalFunction(LogicalFunction haystack, LogicalFunction needle);
    [[nodiscard]] bool operator==(const ContainsLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] ContainsLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] ContainsLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    LogicalFunction haystack;
    LogicalFunction needle;
    DataType dataType;

    friend struct Reflector<ContainsLogicalFunction>;
};

template <>
struct Reflector<ContainsLogicalFunction>
{
    Reflected operator()(const ContainsLogicalFunction& function) const;
};

template <>
struct Unreflector<ContainsLogicalFunction>
{
    ContainsLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<ContainsLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedContainsLogicalFunction
{
    std::optional<LogicalFunction> haystack;
    std::optional<LogicalFunction> needle;
};
}

FMT_OSTREAM(NES::ContainsLogicalFunction);
