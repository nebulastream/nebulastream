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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <sys/types.h>

#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ODBCTypeMappings.hpp>

namespace NES
{

CType::CType(SQLSMALLINT type) : type(type)
{
    if (!isValidCType(type))
    {
        throw CannotOpenSource("Unsupported CType: {}", type);
    }
}

std::chrono::system_clock::time_point getTimestamp(const SQL_TIMESTAMP_STRUCT& ts)
{
    std::chrono::year_month_day ymd{std::chrono::year{ts.year}, std::chrono::month{ts.month}, std::chrono::day{ts.day}};
    if (!ymd.ok())
    {
        throw CannotOpenSource("Invalid date: {}-{}-{}", ts.year, ts.month, ts.day);
    }

    std::chrono::sys_days days{ymd}; // days since 1970-01-01 UTC
    auto tod = std::chrono::hours{ts.hour} + std::chrono::minutes{ts.minute} + std::chrono::seconds{ts.second}
        + std::chrono::nanoseconds{ts.fraction};
    return days + tod;
}

SQL_TIMESTAMP_STRUCT converToTimestamp(const std::chrono::system_clock::time_point& now)
{
    using namespace std::chrono;
    auto days = floor<std::chrono::days>(now);
    year_month_day ymd{days};
    hh_mm_ss tod{now - days};

    SQL_TIMESTAMP_STRUCT out;
    out.year = static_cast<SQLSMALLINT>(int{ymd.year()});
    out.month = static_cast<SQLUSMALLINT>(unsigned{ymd.month()});
    out.day = static_cast<SQLUSMALLINT>(unsigned{ymd.day()});
    out.hour = static_cast<SQLUSMALLINT>(tod.hours().count());
    out.minute = static_cast<SQLUSMALLINT>(tod.minutes().count());
    out.second = static_cast<SQLUSMALLINT>(tod.seconds().count());
    /// fraction is in nanoseconds per the ODBC spec; carry the sub-second
    /// part of the wall clock so windowing queries don't lose precision
    /// against DBs that store microsecond/nanosecond timestamps (e.g. postgres).
    out.fraction = static_cast<SQLUINTEGER>(duration_cast<nanoseconds>(tod.subseconds()).count());
    return out;
}

uint64_t toNESTimestamp(const std::chrono::system_clock::time_point& timestamp)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(timestamp.time_since_epoch()).count();
}

ColumnIndex::ColumnIndex(size_t index) : index(index + 1)
{
}

std::expected<ColumnMapping, std::string> lookupColumnMapping(ColumnIndex index, const DataType& type, const SQLType& sqlType)
{
    if (sqlType.isNullable && !type.nullable)
    {
        return std::unexpected{"Cannot bind nullable SQL type to non nullable NES Type"};
    }
    auto conversion = conversions.find({sqlType.sqlType, type.type});
    if (conversion == conversions.end())
    {
        return std::unexpected{"Cannot bind SQLType {} to NES type {}."};
    }
    return ColumnMapping{index, type, conversion->second};
}
}
