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
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

/// printf-style varsized formatter.
/// FMT(VARSIZED("a={}, b={}"), x, y) → VARSIZED concatenation of literal segments
/// and the stringified arguments. The first child must be a constant VARSIZED string;
/// subsequent children fill the `{}` placeholders left-to-right. During data type
/// inference, non-VARSIZED placeholder children are wrapped in to_string(...) so the
/// physical implementation only ever concatenates VARSIZED fragments.
class FmtLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "FMT";

    explicit FmtLogicalFunction(std::vector<LogicalFunction> children);

    [[nodiscard]] bool operator==(const FmtLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] FmtLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] FmtLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    std::vector<LogicalFunction> children;

    friend Reflector<FmtLogicalFunction>;
};

template <>
struct Reflector<FmtLogicalFunction>
{
    Reflected operator()(const FmtLogicalFunction& function) const;
};

template <>
struct Unreflector<FmtLogicalFunction>
{
    FmtLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<FmtLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedFmtLogicalFunction
{
    std::vector<LogicalFunction> children;
};
}

FMT_OSTREAM(NES::FmtLogicalFunction);
