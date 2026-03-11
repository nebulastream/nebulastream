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

/// Logical function that decodes a VARSIZED base64 string into raw VARSIZED bytes.
class From_Base64LogicalFunction final
{
public:
    static constexpr std::string_view NAME = "From_Base64";

    explicit From_Base64LogicalFunction(const LogicalFunction& child);

    [[nodiscard]] bool operator==(const From_Base64LogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] From_Base64LogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] From_Base64LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    LogicalFunction child;

    friend Reflector<From_Base64LogicalFunction>;
};

static_assert(LogicalFunctionConcept<From_Base64LogicalFunction>);

template <>
struct Reflector<From_Base64LogicalFunction>
{
    Reflected operator()(const From_Base64LogicalFunction& function) const;
};

template <>
struct Unreflector<From_Base64LogicalFunction>
{
    From_Base64LogicalFunction operator()(const Reflected& reflected) const;
};
}

namespace NES::detail
{
struct ReflectedFrom_Base64LogicalFunction
{
    std::optional<LogicalFunction> child;
};
}

FMT_OSTREAM(NES::From_Base64LogicalFunction);
