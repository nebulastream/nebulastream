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

#include <cstdint>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <function.hpp>
#include <val_arith.hpp>

namespace NES
{

namespace
{
/// Writes a two-digit zero-padded value.
void writeTwoDigits(char* p, uint32_t v)
{
    p[0] = static_cast<char>('0' + (v / 10));
    p[1] = static_cast<char>('0' + (v % 10));
}

/// Formats milliseconds-since-epoch as a fixed 24-char ISO-8601 UTC string:
/// YYYY-MM-DDTHH:MM:SS.mmmZ. Hand-rolled to avoid std::format's per-row runtime
/// format-string parsing + locale/chrono machinery (measured ~150x faster), which
/// otherwise single-thread-bottlenecks any CASTFROMUNIXTS query. Byte-exact with
/// std::format's "{:%FT%T}Z" for all inputs in [0, 253402300799999] (epoch .. year
/// 9999); larger inputs would overflow the fixed 24-byte buffer either way.
void formatIso8601Utc(const uint64_t milliSecondsSinceEpoch, char* out)
{
    const auto msOfSecond = static_cast<uint32_t>(milliSecondsSinceEpoch % 1000);
    const uint64_t totalSeconds = milliSecondsSinceEpoch / 1000;
    const auto secondOfDay = static_cast<uint32_t>(totalSeconds % 86400);
    const auto daysSinceEpoch = static_cast<int64_t>(totalSeconds / 86400);

    const uint32_t second = secondOfDay % 60;
    const uint32_t totalMinutes = secondOfDay / 60;
    const uint32_t minute = totalMinutes % 60;
    const uint32_t hour = totalMinutes / 60;

    /// Howard Hinnant's civil_from_days: days-since-1970-01-01 -> year/month/day (proleptic Gregorian, UTC).
    const int64_t z = daysSinceEpoch + 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const auto dayOfEra = static_cast<uint32_t>(z - (era * 146097)); /// [0, 146096]
    const uint32_t yearOfEra = (dayOfEra - (dayOfEra / 1460) + (dayOfEra / 36524) - (dayOfEra / 146096)) / 365; /// [0, 399]
    const int64_t yearBase = static_cast<int64_t>(yearOfEra) + (era * 400);
    const uint32_t dayOfYear = dayOfEra - ((365 * yearOfEra) + (yearOfEra / 4) - (yearOfEra / 100)); /// [0, 365]
    const uint32_t monthPrime = ((5 * dayOfYear) + 2) / 153; /// [0, 11] (March-based)
    const uint32_t day = (dayOfYear - (((153 * monthPrime) + 2) / 5)) + 1; /// [1, 31]
    const uint32_t month = monthPrime < 10 ? monthPrime + 3 : monthPrime - 9; /// [1, 12]
    const auto year = static_cast<uint32_t>(yearBase + (month <= 2 ? 1 : 0));

    out[0] = static_cast<char>('0' + (year / 1000 % 10));
    out[1] = static_cast<char>('0' + (year / 100 % 10));
    out[2] = static_cast<char>('0' + (year / 10 % 10));
    out[3] = static_cast<char>('0' + (year % 10));
    out[4] = '-';
    writeTwoDigits(out + 5, month);
    out[7] = '-';
    writeTwoDigits(out + 8, day);
    out[10] = 'T';
    writeTwoDigits(out + 11, hour);
    out[13] = ':';
    writeTwoDigits(out + 14, minute);
    out[16] = ':';
    writeTwoDigits(out + 17, second);
    out[19] = '.';
    out[20] = static_cast<char>('0' + (msOfSecond / 100 % 10));
    out[21] = static_cast<char>('0' + (msOfSecond / 10 % 10));
    out[22] = static_cast<char>('0' + (msOfSecond % 10));
    out[23] = 'Z';
}
}

CastFromUnixTimestampPhysicalFunction::CastFromUnixTimestampPhysicalFunction(PhysicalFunction childFunction, DataType outputType)
    : outputType(std::move(outputType)), childFunction(std::move(childFunction))
{
}

VarVal CastFromUnixTimestampPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childFunction.execute(record, arena);
    if (value.isNullable())
    {
        if (value.isNull())
        {
            return VarVal{VariableSizedData{nullptr, 0}, true, true};
        }
    }

    /// ISO-8601 UTC format: YYYY-MM-DDTHH:MM:SS.mmmZ (24 chars)
    const auto milliSeconds = value.getRawValueAs<nautilus::val<uint64_t>>();
    constexpr uint32_t iso8601OutputLen = 24;
    const nautilus::val<uint32_t> isoTimestampLength{iso8601OutputLen};
    auto timestampAsIso8601 = arena.allocateVariableSizedData(isoTimestampLength);
    const auto payload = timestampAsIso8601.getContent();

    nautilus::invoke(
        +[](const uint64_t milliSecondsSinceEpoch, char* outTimestampIso8601Utc)
        { formatIso8601Utc(milliSecondsSinceEpoch, outTimestampIso8601Utc); },
        milliSeconds,
        payload);

    return VarVal{timestampAsIso8601, value.isNullable(), false};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterCastFromUnixTsPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 1, "CastFromUnixTimestampPhysicalFunction must have exactly one child function");

    return CastFromUnixTimestampPhysicalFunction(args.childFunctions[0], args.outputType);
}

}
