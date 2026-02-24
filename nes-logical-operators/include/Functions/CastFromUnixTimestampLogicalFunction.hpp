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

/// Converts a unix timestamp in milliseconds (UINT64) to an ISO-8601 UTC timestamp string (VARSIZED).
class CastFromUnixTimestampLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "CastFromUnixTs";

    explicit CastFromUnixTimestampLogicalFunction(LogicalFunction child);

    [[nodiscard]] bool operator==(const CastFromUnixTimestampLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] CastFromUnixTimestampLogicalFunction withDataType(const DataType& dataType) const;

    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] CastFromUnixTimestampLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType outputType;
    LogicalFunction child;

    friend Reflector<CastFromUnixTimestampLogicalFunction>;
};

template <>
struct Reflector<CastFromUnixTimestampLogicalFunction>
{
    Reflected operator()(const CastFromUnixTimestampLogicalFunction& function) const;
};

template <>
struct Unreflector<CastFromUnixTimestampLogicalFunction>
{
    CastFromUnixTimestampLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<CastFromUnixTimestampLogicalFunction>);

} /// namespace NES

namespace NES::detail
{
struct ReflectedCastFromUnixTimestampLogicalFunction
{
    std::optional<LogicalFunction> child;
    /// NOTE: intentionally NO outputType here (we infer it)
};
} /// namespace NES::detail

FMT_OSTREAM(NES::CastFromUnixTimestampLogicalFunction);
