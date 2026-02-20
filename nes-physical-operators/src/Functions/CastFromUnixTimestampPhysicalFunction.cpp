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
#include <utility>

#include <date/date.h>

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val.hpp>

namespace NES
{
namespace
{
/// Using a iso8601 format that creates the following strings: 2026-02-20T15:30:45.123Z
constexpr std::string_view iso8601Format{"%04d-%02u-%02uT%02d:%02d:%02d.%03dZ"};
constexpr auto iso8601FormatLength = iso8601Format.length();

void writeZuluTimestamp(char* out, uint64_t milliSecondsSinceEpoch)
{
    const date::sys_time tp{std::chrono::milliseconds{milliSecondsSinceEpoch}};
    const auto tpSec = date::floor<std::chrono::seconds>(tp);
    const auto msPart = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(tp - tpSec).count());

    const auto dayPoint = date::floor<date::days>(tpSec);
    const date::year_month_day ymd{dayPoint};
    const auto tod = date::make_time(tpSec - dayPoint);

    const int year = static_cast<int>(ymd.year());
    const unsigned month = static_cast<unsigned>(ymd.month());
    const unsigned day = static_cast<unsigned>(ymd.day());

    const int hh = static_cast<int>(tod.hours().count());
    const int mm = static_cast<int>(tod.minutes().count());
    const int ss = static_cast<int>(tod.seconds().count());

    std::snprintf(out, iso8601FormatLength + 1, iso8601Format.data(), year, month, day, hh, mm, ss, msPart);
}
}

CastFromUnixTimestampPhysicalFunction::CastFromUnixTimestampPhysicalFunction(PhysicalFunction childFunction, DataType outputType)
    : outputType(std::move(outputType)), childFunction(std::move(childFunction))
{
}

VarVal CastFromUnixTimestampPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childFunction.execute(record, arena);
    const auto timestampInMilliSeconds = value.cast<nautilus::val<uint64_t>>();
    auto timestampAsISO8601 = arena.allocateVariableSizedData(nautilus::val<uint32_t>(iso8601FormatLength));
    nautilus::invoke(
        +[](const uint64_t ms, int8_t* payload) { writeZuluTimestamp(reinterpret_cast<char*>(payload), ms); }, timestampInMilliSeconds, timestampAsISO8601.getContent());
    VarVal(nautilus::val<uint32_t>(iso8601FormatLength)).writeToMemory(timestampAsISO8601.getReference());
    return timestampAsISO8601;
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterCastFromUnixTsPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 1, "CastFromUnixTimestampPhysicalFunction must have exactly one child function");

    return CastFromUnixTimestampPhysicalFunction(args.childFunctions[0], args.outputType);
}

}
