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

#include <Functions/CastFromUnixTimestampPhysicalFunction.hpp>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <utility>

#include <date/date.h>

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val.hpp>

namespace NES
{
namespace
{

/// ISO-8601 UTC format: YYYY-MM-DDTHH:MM:SS.mmmZ (24 chars)
constexpr uint64_t iso8601OutputLen = 24;
constexpr std::string_view iso8601Format{"%04d-%02u-%02uT%02d:%02d:%02d.%03dZ"};

void writeZuluTimestampExact(char* out24, uint64_t milliSecondsSinceEpoch)
{
    /// Convert epoch milliseconds to a UTC time_point
    const date::sys_time utcTimePoint{std::chrono::milliseconds{milliSecondsSinceEpoch}};

    /// Split into whole seconds + millisecond remainder
    const auto utcSeconds = date::floor<std::chrono::seconds>(utcTimePoint);
    const int millisecondPart = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(utcTimePoint - utcSeconds).count());

    /// Split into calendar date + time-of-day (UTC)
    const auto utcMidnight = date::floor<date::days>(utcSeconds);
    const date::year_month_day utcDate{utcMidnight};
    const auto utcTimeOfDay = date::make_time(utcSeconds - utcMidnight);

    /// Extract time components
    const int year = static_cast<int>(utcDate.year());
    const unsigned month = unsigned{utcDate.month()};
    const unsigned day = unsigned{utcDate.day()};

    const int hh = static_cast<int>(utcTimeOfDay.hours().count());
    const int mm = static_cast<int>(utcTimeOfDay.minutes().count());
    const int ss = static_cast<int>(utcTimeOfDay.seconds().count());

    /// snprintf needs a null-terminated buffer; output is fixed-length (24 bytes) without '\0'
    char iso8601NullTerminated[iso8601OutputLen + 1];
    std::snprintf(
        iso8601NullTerminated, sizeof(iso8601NullTerminated), iso8601Format.data(), year, month, day, hh, mm, ss, millisecondPart);

    std::memcpy(out24, iso8601NullTerminated, iso8601OutputLen);
}
}

CastFromUnixTimestampPhysicalFunction::CastFromUnixTimestampPhysicalFunction(PhysicalFunction childFunction, DataType outputType)
    : outputType(std::move(outputType)), childFunction(std::move(childFunction))
{
}

VarVal CastFromUnixTimestampPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childFunction.execute(record, arena);
    const auto ms = value.cast<nautilus::val<uint64_t>>();

    const auto isoTimestampLength = nautilus::val<uint32_t>(iso8601OutputLen);
    auto timestampAsIso8601 = arena.allocateVariableSizedData(isoTimestampLength);
    const auto payload = timestampAsIso8601.getContent();

    nautilus::invoke(
        +[](uint64_t millis, int8_t* buf)
        {
            writeZuluTimestampExact(reinterpret_cast<char*>(buf), millis); /// writes 24 bytes
        },
        ms,
        payload);

    return timestampAsIso8601;
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterCastFromUnixTsPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 1, "CastFromUnixTimestampPhysicalFunction must have exactly one child function");

    return CastFromUnixTimestampPhysicalFunction(args.childFunctions[0], args.outputType);
}

}
