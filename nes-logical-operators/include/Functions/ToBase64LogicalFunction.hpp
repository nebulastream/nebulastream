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

/// Logical function that encodes VARSIZED raw bytes into a VARSIZED base64 string.
class ToBase64LogicalFunction final
{
public:
    static constexpr std::string_view NAME = "TO_BASE64";

    explicit ToBase64LogicalFunction(const LogicalFunction& child);

    [[nodiscard]] bool operator==(const ToBase64LogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] ToBase64LogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] ToBase64LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    LogicalFunction child;

    friend Reflector<ToBase64LogicalFunction>;
};

static_assert(LogicalFunctionConcept<ToBase64LogicalFunction>);

template <>
struct Reflector<ToBase64LogicalFunction>
{
    Reflected operator()(const ToBase64LogicalFunction& function) const;
};

template <>
struct Unreflector<ToBase64LogicalFunction>
{
    ToBase64LogicalFunction operator()(const Reflected& reflected) const;
};
}

namespace NES::detail
{
struct ReflectedToBase64LogicalFunction
{
    std::optional<LogicalFunction> child;
};
}

FMT_OSTREAM(NES::ToBase64LogicalFunction);
