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

#include <Functions/CastToUnixTimestampPhysicalFunction.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <istream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Strings.hpp>
#include <date/date.h>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <function.hpp>

namespace NES
{
namespace
{

/// Checks that from_stream consumed the entire input and returns the parsed milliseconds.
/// Returns std::nullopt on parse failure or trailing non-whitespace characters.
std::optional<uint64_t>
toMilliSecondsIfFullyParsed(std::istringstream& iss, std::chrono::milliseconds epochDuration, std::string_view input)
{
    if (iss.fail())
    {
        return std::nullopt;
    }
    /// from_stream may leave trailing whitespace; consume it so only real leftovers fail.
    iss >> std::ws;
    if (!iss.eof())
    {
        return std::nullopt;
    }
    if (epochDuration.count() < 0)
    {
        throw FormattingError("CastToUnixTs: pre-epoch timestamp is not supported: '{}'", input);
    }
    return static_cast<uint64_t>(epochDuration.count());
}

uint64_t parseISO8601TimestampToUnixMilliSeconds(std::string_view iso8601Timestamp)
{
    const auto trimmed = NES::trimWhiteSpaces(iso8601Timestamp);
    if (trimmed.empty())
    {
        throw FormattingError("CastToUnixTs: cannot convert empty timestamp: '{}'", iso8601Timestamp);
    }

    static constexpr std::array<std::string_view, 5> FormatsWithTz = {
        "%FT%T%Ez",
        "%FT%T.%f%Ez",
        "%FT%TZ",
        "%FT%T.%fZ",
        "%a, %d %b %Y %T GMT",
    };

    static constexpr std::array<std::string_view, 4> FormatsWithoutTz = {
        "%F %T",
        "%F %T.%f",
        "%FT%T",
        "%FT%T.%f",
    };

    /// Try formats that carry an explicit timezone / offset
    for (const auto& fmt : FormatsWithTz)
    {
        std::istringstream iss{std::string{trimmed}};
        std::string abbrev;
        std::chrono::minutes offset{0};
        date::sys_time<std::chrono::milliseconds> timepoint;
        /// NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage) not possible to provide size information to from_stream
        date::from_stream(iss, fmt.data(), timepoint, &abbrev, &offset);
        if (auto milliSeconds = toMilliSecondsIfFullyParsed(iss, timepoint.time_since_epoch(), iso8601Timestamp))
        {
            return *milliSeconds;
        }
    }

    /// Try timezone-free formats (interpreted as UTC)
    for (const auto& fmt : FormatsWithoutTz)
    {
        std::istringstream iss{std::string{trimmed}};
        date::local_time<std::chrono::milliseconds> timepoint;
        /// NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage) not possible to provide size information to from_stream
        date::from_stream(iss, fmt.data(), timepoint);
        if (auto milliSeconds = toMilliSecondsIfFullyParsed(iss, timepoint.time_since_epoch(), iso8601Timestamp))
        {
            return *milliSeconds;
        }
    }

    throw FormattingError("CastToUnixTs: unsupported timestamp format: '{}'", iso8601Timestamp);
}

}

CastToUnixTimestampPhysicalFunction::CastToUnixTimestampPhysicalFunction(PhysicalFunction childFunction, DataType outputType)
    : outputType(std::move(outputType)), childFunction(std::move(childFunction))
{
}

VarVal CastToUnixTimestampPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childFunction.execute(record, arena);
    if (value.isNullable())
    {
        if (value.isNull())
        {
            return VarVal{0, true, true}.castToType(outputType.type);
        }
    }

    const auto var = value.getRawValueAs<VariableSizedData>();
    const auto size = var.getSize();
    const auto ptr = var.getContent();

    const auto parsedMilliSeconds = nautilus::invoke(
        +[](const uint32_t size, const char* iso8601String) -> uint64_t
        {
            const std::string_view iso8601StringView{iso8601String, size};
            return parseISO8601TimestampToUnixMilliSeconds(iso8601StringView);
        },
        size,
        ptr);

    return VarVal{parsedMilliSeconds, value.isNullable(), false}.castToType(outputType.type);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterCastToUnixTsPhysicalFunction(
    PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        physicalFunctionRegistryArguments.childFunctions.size() == 1,
        "CastToUnixTimestampPhysicalFunction must have exactly one child function");

    return CastToUnixTimestampPhysicalFunction(
        physicalFunctionRegistryArguments.childFunctions[0], physicalFunctionRegistryArguments.outputType);
}

}
