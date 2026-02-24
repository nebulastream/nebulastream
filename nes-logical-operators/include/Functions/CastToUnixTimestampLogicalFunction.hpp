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

namespace NES
{

/// Casts the input to a unix timestamp in milliseconds.
class CastToUnixTimestampLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "CastToUnixTs";

    CastToUnixTimestampLogicalFunction(LogicalFunction child);

    [[nodiscard]] bool operator==(const CastToUnixTimestampLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] CastToUnixTimestampLogicalFunction withDataType(const DataType& dataType) const;

    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] CastToUnixTimestampLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType outputType;
    LogicalFunction child;

    friend Reflector<CastToUnixTimestampLogicalFunction>;
};

static_assert(LogicalFunctionConcept<CastToUnixTimestampLogicalFunction>);

template <>
struct Reflector<CastToUnixTimestampLogicalFunction>
{
    Reflected operator()(const CastToUnixTimestampLogicalFunction& function) const;
};

template <>
struct Unreflector<CastToUnixTimestampLogicalFunction>
{
    CastToUnixTimestampLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<CastToUnixTimestampLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedCastToUnixTimestampLogicalFunction
{
    std::optional<LogicalFunction> child;
};
}

FMT_OSTREAM(NES::CastToUnixTimestampLogicalFunction);
