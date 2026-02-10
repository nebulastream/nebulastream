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
class SqrtLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "Sqrt";

    explicit SqrtLogicalFunction(const LogicalFunction& child);

    [[nodiscard]] bool operator==(const SqrtLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] SqrtLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] SqrtLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    LogicalFunction child;

    friend Reflector<SqrtLogicalFunction>;
};

template <>
struct Reflector<SqrtLogicalFunction>
{
    Reflected operator()(const SqrtLogicalFunction& function) const;
};

template <>
struct Unreflector<SqrtLogicalFunction>
{
    SqrtLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<SqrtLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedSqrtLogicalFunction
{
    std::optional<LogicalFunction> child;
};
}

FMT_OSTREAM(NES::SqrtLogicalFunction);
