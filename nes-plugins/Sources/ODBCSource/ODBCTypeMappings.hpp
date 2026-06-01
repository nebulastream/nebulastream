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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <DataTypes/DataType.hpp>
#include <fmt/format.h>
/// folly's Hash.h provides std::hash<std::pair<...>> via a partial specialization;
/// clang-tidy's include-cleaner doesn't see specialization-only usage and flags
/// the include as unused.
/// NOLINTNEXTLINE(misc-include-cleaner)
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{

/// SQL_C_WCHAR comes from <sqltypes.h> which is included; clang-tidy's symbol mapping
/// misses some unixODBC macros, so band the whole switch.
/// NOLINTBEGIN(misc-include-cleaner)
constexpr bool isValidCType(SQLSMALLINT cType)
{
    switch (cType)
    {
        case SQL_C_CHAR:
        case SQL_C_WCHAR:
        case SQL_C_BIT:
        case SQL_C_STINYINT:
        case SQL_C_UTINYINT:
        case SQL_C_SSHORT:
        case SQL_C_USHORT:
        case SQL_C_SLONG:
        case SQL_C_ULONG:
        case SQL_C_SBIGINT:
        case SQL_C_UBIGINT:
        case SQL_C_FLOAT:
        case SQL_C_DOUBLE:
        case SQL_C_BINARY:
        case SQL_C_TYPE_DATE:
        case SQL_C_TYPE_TIME:
        case SQL_C_TYPE_TIMESTAMP:
        case SQL_C_GUID:
        case SQL_C_DEFAULT:
            return true;
        default:
            return false;
    }
}

/// NOLINTEND(misc-include-cleaner)

struct CType
{
    /// The ctor body throws on an invalid value, so the conversion is validated, not silent.
    /// NOLINTNEXTLINE(google-explicit-constructor): implicit construction from SQLSMALLINT keeps the conversions-table syntax `{.cType = SQL_C_X}` readable
    CType(SQLSMALLINT type);

    CType() = default;

    SQLSMALLINT type = SQL_TYPE_NULL;
};

struct Direct
{
    CType cType;
};

using FixupFunction = std::function<void(std::span<const std::byte>, std::span<std::byte>)>;

struct Fixup
{
    CType cType;
    size_t fixupSize;
    FixupFunction fixupFunction;
};

struct VarSized
{
    CType cType;
};

std::chrono::system_clock::time_point getTimestamp(const SQL_TIMESTAMP_STRUCT& ts);

SQL_TIMESTAMP_STRUCT converToTimestamp(const std::chrono::system_clock::time_point& now);

uint64_t toNESTimestamp(const std::chrono::system_clock::time_point& timestamp);

using Translation = std::variant<Direct, Fixup, VarSized>;

/// `conversions` is a lookup table populated at static-init. cert-err58-cpp fires because the
/// unordered_map ctor may throw bad_alloc — at static-init this is uncatchable. The lint is
/// project-wide idiomatic on ConfigParameter tables (see ODBCSink.hpp); same applies here.
/// Several SQL_W* / SQL_C_WCHAR macros come from <sqltypes.h> conditionally and
/// clang-tidy's include-cleaner doesn't always see them.
/// NOLINTBEGIN(cert-err58-cpp,misc-include-cleaner)
static const std::unordered_map<std::pair<SQLSMALLINT, DataType::Type>, Translation> conversions = {
    {{SQL_CHAR, DataType::Type::VARSIZED}, VarSized{.cType = SQL_C_CHAR}},
    {{SQL_VARCHAR, DataType::Type::VARSIZED}, VarSized{.cType = SQL_C_CHAR}},
    {{SQL_WVARCHAR, DataType::Type::VARSIZED}, VarSized{.cType = SQL_C_CHAR}},
    {{SQL_BINARY, DataType::Type::VARSIZED}, VarSized{.cType = SQL_C_BINARY}},
    {{SQL_INTEGER, DataType::Type::INT8}, Direct{.cType = SQL_C_STINYINT}},
    {{SQL_INTEGER, DataType::Type::INT16}, Direct{.cType = SQL_C_SSHORT}},
    {{SQL_INTEGER, DataType::Type::INT32}, Direct{.cType = SQL_C_SLONG}},
    {{SQL_INTEGER, DataType::Type::INT64}, Direct{.cType = SQL_C_SBIGINT}},
    {{SQL_INTEGER, DataType::Type::UINT8}, Direct{.cType = SQL_C_UTINYINT}},
    {{SQL_INTEGER, DataType::Type::UINT16}, Direct{.cType = SQL_C_USHORT}},
    {{SQL_INTEGER, DataType::Type::UINT32}, Direct{.cType = SQL_C_ULONG}},
    {{SQL_INTEGER, DataType::Type::UINT64}, Direct{.cType = SQL_C_UBIGINT}},
    /// The block above keys every integer on SQL_INTEGER, but drivers report
    /// the *exact* width: psqlodbc maps Postgres BIGINT -> SQL_BIGINT,
    /// SMALLINT -> SQL_SMALLINT, DOUBLE PRECISION -> SQL_DOUBLE, REAL ->
    /// SQL_REAL (SQL Server's driver reports the same exact-width codes). Map
    /// each to its matching NES type so common numeric columns bind, not just
    /// 32-bit INTEGER.
    {{SQL_BIGINT, DataType::Type::INT64}, Direct{.cType = SQL_C_SBIGINT}},
    {{SQL_BIGINT, DataType::Type::UINT64}, Direct{.cType = SQL_C_UBIGINT}},
    {{SQL_SMALLINT, DataType::Type::INT16}, Direct{.cType = SQL_C_SSHORT}},
    {{SQL_SMALLINT, DataType::Type::UINT16}, Direct{.cType = SQL_C_USHORT}},
    {{SQL_TINYINT, DataType::Type::INT8}, Direct{.cType = SQL_C_STINYINT}},
    {{SQL_TINYINT, DataType::Type::UINT8}, Direct{.cType = SQL_C_UTINYINT}},
    {{SQL_DOUBLE, DataType::Type::FLOAT64}, Direct{.cType = SQL_C_DOUBLE}},
    {{SQL_FLOAT, DataType::Type::FLOAT64}, Direct{.cType = SQL_C_DOUBLE}},
    {{SQL_REAL, DataType::Type::FLOAT32}, Direct{.cType = SQL_C_FLOAT}},
    {{SQL_LONGVARCHAR, DataType::Type::VARSIZED}, VarSized{.cType = SQL_C_CHAR}},
    {{SQL_WCHAR, DataType::Type::VARSIZED}, VarSized{.cType = SQL_C_CHAR}},
    {{SQL_WLONGVARCHAR, DataType::Type::VARSIZED}, VarSized{.cType = SQL_C_CHAR}},
    {{SQL_VARBINARY, DataType::Type::VARSIZED}, VarSized{.cType = SQL_C_BINARY}},
    {{SQL_LONGVARBINARY, DataType::Type::VARSIZED}, VarSized{.cType = SQL_C_BINARY}},
    {{SQL_TYPE_TIMESTAMP, DataType::Type::UINT64},
     Fixup{
         .cType = SQL_C_TYPE_TIMESTAMP,
         .fixupSize = sizeof(SQL_TIMESTAMP_STRUCT),
         .fixupFunction = std::function(
             [](std::span<const std::byte> timestruct, std::span<std::byte> fixed)
             {
                 PRECONDITION(timestruct.size() >= sizeof(SQL_TIMESTAMP_STRUCT), "size missmatch");
                 PRECONDITION(fixed.size() >= sizeof(int64_t), "size missmatch");

                 SQL_TIMESTAMP_STRUCT ts;
                 std::memcpy(&ts, timestruct.data(), sizeof(SQL_TIMESTAMP_STRUCT));
                 auto timestamp = toNESTimestamp(getTimestamp(ts));
                 std::memcpy(fixed.data(), &timestamp, sizeof(timestamp));
             })}},
};

/// NOLINTEND(cert-err58-cpp,misc-include-cleaner)

constexpr std::string_view sqlTypeName(SQLSMALLINT sqlType) noexcept
{
    switch (sqlType)
    {
        /// Character types
        case SQL_CHAR:
            return "CHAR";
        case SQL_VARCHAR:
            return "VARCHAR";
        case SQL_LONGVARCHAR:
            return "LONGVARCHAR";
        case SQL_WCHAR:
            return "WCHAR";
        case SQL_WVARCHAR:
            return "WVARCHAR";
        case SQL_WLONGVARCHAR:
            return "WLONGVARCHAR";

        /// Exact numeric
        case SQL_DECIMAL:
            return "DECIMAL";
        case SQL_NUMERIC:
            return "NUMERIC";
        case SQL_SMALLINT:
            return "SMALLINT";
        case SQL_INTEGER:
            return "INTEGER";
        case SQL_TINYINT:
            return "TINYINT";
        case SQL_BIGINT:
            return "BIGINT";
        case SQL_BIT:
            return "BIT";

        /// Approximate numeric
        case SQL_REAL:
            return "REAL"; /// 4-byte float
        case SQL_FLOAT:
            return "FLOAT"; /// typically 8-byte
        case SQL_DOUBLE:
            return "DOUBLE"; /// 8-byte

        /// Binary
        case SQL_BINARY:
            return "BINARY";
        case SQL_VARBINARY:
            return "VARBINARY";
        case SQL_LONGVARBINARY:
            return "LONGVARBINARY";

        /// Date / time
        case SQL_TYPE_DATE:
            return "DATE";
        case SQL_TYPE_TIME:
            return "TIME";
        case SQL_TYPE_TIMESTAMP:
            return "TIMESTAMP";

        /// Interval
        case SQL_INTERVAL_YEAR:
            return "INTERVAL_YEAR";
        case SQL_INTERVAL_MONTH:
            return "INTERVAL_MONTH";
        case SQL_INTERVAL_DAY:
            return "INTERVAL_DAY";
        case SQL_INTERVAL_HOUR:
            return "INTERVAL_HOUR";
        case SQL_INTERVAL_MINUTE:
            return "INTERVAL_MINUTE";
        case SQL_INTERVAL_SECOND:
            return "INTERVAL_SECOND";
        case SQL_INTERVAL_YEAR_TO_MONTH:
            return "INTERVAL_YEAR_TO_MONTH";
        case SQL_INTERVAL_DAY_TO_HOUR:
            return "INTERVAL_DAY_TO_HOUR";
        case SQL_INTERVAL_DAY_TO_MINUTE:
            return "INTERVAL_DAY_TO_MINUTE";
        case SQL_INTERVAL_DAY_TO_SECOND:
            return "INTERVAL_DAY_TO_SECOND";
        case SQL_INTERVAL_HOUR_TO_MINUTE:
            return "INTERVAL_HOUR_TO_MINUTE";
        case SQL_INTERVAL_HOUR_TO_SECOND:
            return "INTERVAL_HOUR_TO_SECOND";
        case SQL_INTERVAL_MINUTE_TO_SECOND:
            return "INTERVAL_MINUTE_TO_SECOND";

        /// Other
        case SQL_GUID:
            return "GUID";

        default:
            return "UNKNOWN";
    }
}

struct SQLType
{
    SQLSMALLINT sqlType;
    bool isNullable;
};

struct ColumnIndex
{
    explicit ColumnIndex(size_t index);

    size_t index;
    auto operator<=>(const ColumnIndex& other) const = default;
};

struct ColumnMapping
{
    ColumnIndex columnIndex;
    DataType nesType;
    Translation translation;
};

std::expected<ColumnMapping, std::string> lookupColumnMapping(ColumnIndex index, const DataType& type, const SQLType& sqlType);

inline auto format_as(SQLType type)
{
    return fmt::format("{}({})", sqlTypeName(type.sqlType), type.isNullable ? "nullable" : "non-nullable");
}
}
