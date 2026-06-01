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

#include <ODBCTypeMappings.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <string>
#include <sqltypes.h>

#include <DataTypes/DataType.hpp>

#include <fmt/format.h>
#include <ErrorHandling.hpp>

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
    const std::chrono::year_month_day ymd{std::chrono::year{ts.year}, std::chrono::month{ts.month}, std::chrono::day{ts.day}};
    if (!ymd.ok())
    {
        throw CannotOpenSource("Invalid date: {}-{}-{}", ts.year, ts.month, ts.day);
    }

    const std::chrono::sys_days days{ymd}; /// days since 1970-01-01 UTC
    const auto tod = std::chrono::hours{ts.hour} + std::chrono::minutes{ts.minute} + std::chrono::seconds{ts.second}
        + std::chrono::nanoseconds{ts.fraction};
    /// `days + tod` is nanosecond-resolution; our libc++ `system_clock::time_point`
    /// is microsecond-resolution, so the implicit narrowing conversion is
    /// ill-formed. Truncate explicitly to the clock's own duration.
    return std::chrono::time_point_cast<std::chrono::system_clock::duration>(days + tod);
}

SQL_TIMESTAMP_STRUCT convertToTimestamp(const std::chrono::system_clock::time_point& now)
{
    using std::chrono::duration_cast;
    using std::chrono::floor;
    using std::chrono::hh_mm_ss;
    using std::chrono::nanoseconds;
    using std::chrono::year_month_day;
    /// NOLINTNEXTLINE(misc-include-cleaner): std::chrono::days is provided by <chrono> (already included); the symbol mapping misses C++20 calendar additions.
    const auto days = floor<std::chrono::days>(now);
    const year_month_day ymd{days};
    const hh_mm_ss tod{now - days};

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
        return std::unexpected{fmt::format(
            "Cannot bind SQLType {} (code {}) to NES type code {}.",
            sqlTypeName(sqlType.sqlType),
            sqlType.sqlType,
            static_cast<int>(type.type))};
    }
    return ColumnMapping{.columnIndex = index, .nesType = type, .translation = conversion->second};
}
}
