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

#include <cctype>
#include <chrono>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

#include <date/date.h>

#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{
namespace
{

uint64_t parseISO8601TimestampToUnixMilliSeconds(std::string_view iso8601Timestamp)
{
    const auto timestampWithoutWhiteSpace = NES::trimWhiteSpaces(iso8601Timestamp);

    if (timestampWithoutWhiteSpace.empty())
    {
        throw FormattingError("CastToUnixTs: cannot convert empty timestamp: '{}'", iso8601Timestamp);
    }

    static constexpr const char* formats_with_tz[] = {
        "%FT%T%Ez",
        "%FT%T.%f%Ez",
        "%FT%TZ",
        "%FT%T.%fZ",
        "%a, %d %b %Y %T GMT",
    };

    static constexpr const char* formats_without_tz[] = {
        "%F %T",
        "%F %T.%f",
        "%FT%T",
        "%FT%T.%f",
    };

    /// First, we try out formats containing a timezone
    for (const char* fmt : formats_with_tz)
    {
        std::istringstream iss(std::string{timestampWithoutWhiteSpace});
        std::string abbrev;
        std::chrono::minutes offset{0};
        date::sys_time<std::chrono::milliseconds> tp;
        date::from_stream(iss, fmt, tp, &abbrev, &offset);

        if (iss.fail())
        {
            continue;
        }

        iss >> std::ws;
        if (!iss.eof())
        {
            continue;
        }

        const auto parsedMilliSeconds = tp.time_since_epoch().count();
        if (parsedMilliSeconds < 0)
        {
            throw FormattingError("CastToUnixTs: pre-epoch timestamp is not supported: '{}'", iso8601Timestamp);
        }
        return static_cast<uint64_t>(parsedMilliSeconds);
    }

    /// Second, we try out formats containing no timezone
    for (const char* fmt : formats_without_tz)
    {
        std::istringstream iss(std::string{timestampWithoutWhiteSpace});
        date::local_time<std::chrono::milliseconds> ltp;
        date::from_stream(iss, fmt, ltp);

        if (iss.fail())
        {
            continue;
        }

        iss >> std::ws;
        if (!iss.eof())
        {
            continue;
        }

        date::sys_time<std::chrono::milliseconds> tp;
        tp = date::sys_time<std::chrono::milliseconds>{ltp.time_since_epoch()};

        const auto parsedMilliSeconds = tp.time_since_epoch().count();
        if (parsedMilliSeconds < 0)
        {
            throw FormattingError("CastToUnixTs: pre-epoch timestamp is not supported: '{}'", iso8601Timestamp);
        }
        return static_cast<uint64_t>(parsedMilliSeconds);
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

    const auto var = value.cast<VariableSizedData>();
    const auto size = var.getSize();
    const auto ptr = var.getContent();

    const auto parsedMilliSeconds = nautilus::invoke(
        +[](const uint32_t size, int8_t* p) -> uint64_t
        {
            const std::string_view sv{reinterpret_cast<const char*>(p), size};
            return parseISO8601TimestampToUnixMilliSeconds(sv);
        },
        size,
        ptr);

    return VarVal{parsedMilliSeconds}.castToType(outputType.type);
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
