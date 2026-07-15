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

#include <ODBCSource.hpp>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <ostream>
#include <ranges>
#include <span>
#include <sstream>
#include <stop_token>
#include <string>
#include <system_error>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Ranges.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <ODBCTypeMappings.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

/// The ODBC C API speaks in `SQLCHAR*` (an unsigned-char alias) and binds parameters / fetches
/// columns through a `void*`/`SQLPOINTER`. Bridging NebulaStream's `std::byte` / `char` buffers
/// to it unavoidably requires reinterpret_cast across that C-API boundary. The check is silenced
/// file-wide for it; every cast here is mandated by the unixODBC headers.
///
/// `rc` (SQLRETURN) is the unixODBC convention and appears at every API call; the
/// readability-identifier-length band keeps grep-ability for ODBC-versed readers.
/// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast,readability-identifier-length)
namespace NES
{

struct ConnectionInformation
{
    std::string server;
    std::string port;
    std::string database;
    std::string username;
    std::string password;
    std::string driver;
    bool trustServerCertificate{};
};

namespace
{
/// The two supported ODBC drivers need different connection-string dialects, so branch on the
/// driver name (Microsoft's driver is "ODBC Driver NN for SQL Server").
///
/// SQL Server (msodbcsql): `SERVER=<host>,<port>` — the driver has no standalone `PORT=` keyword,
/// so the port must be appended to SERVER with a comma. Driver 18 also defaults to
/// Encrypt=Mandatory, so a SQL Server with a self-signed certificate refuses the connection unless
/// `TrustServerCertificate=yes` is set.
///
/// psqlodbc/libpq: `SERVER=<host>;PORT=<port>` — NOT `SERVER=<host>,<port>`. libpq reads a comma in
/// SERVER as a multi-host list and ignores the PORT, so `SERVER=127.0.0.1,1` becomes hosts
/// ["127.0.0.1", "1"] at the default port 5432 — and "1" parses as 0.0.0.1, whose connect hangs
/// where the network drops the SYN (e.g. CI) because there is no login timeout. The semicolon form
/// binds the real host:port and fails fast on a refused/closed port. `TrustServerCertificate` is a
/// SQL-Server-only keyword, so it is not emitted here.
std::string buildConnectionString(const ConnectionInformation& info)
{
    if (info.driver.find("SQL Server") != std::string::npos)
    {
        return fmt::format(
            "DRIVER={{{}}};SERVER={},{};DATABASE={};UID={};PWD={};TrustServerCertificate={}",
            info.driver,
            info.server,
            info.port,
            info.database,
            info.username,
            info.password,
            info.trustServerCertificate ? "yes" : "no");
    }
    return fmt::format(
        "DRIVER={{{}}};SERVER={};PORT={};DATABASE={};UID={};PWD={};CharSet=UTF-8",
        info.driver,
        info.server,
        info.port,
        info.database,
        info.username,
        info.password);
}
}

namespace
{

/// SQLSTATE is a fixed 5-character code; the buffer holds 5 chars + NUL terminator.
constexpr SQLSMALLINT SQL_STATE_BUFFER_SIZE = 6;
/// Buffer size for the connection string the driver echoes back from SQLDriverConnect.
constexpr SQLSMALLINT OUT_CONNECTION_STRING_SIZE = 1024;
/// Column-name buffer size used by SQLDescribeCol.
constexpr SQLSMALLINT COLUMN_NAME_BUFFER_SIZE = 256;
/// ColumnSize=26, DecimalDigits=6 → microsecond-precision timestamp
/// (YYYY-MM-DD HH:MM:SS.ffffff).
constexpr SQLULEN TIMESTAMP_COLUMN_SIZE = 26;
constexpr SQLSMALLINT TIMESTAMP_DECIMAL_DIGITS = 6;
/// The incremental poller binds exactly one `?` parameter: the lower watermark bound. Each poll
/// runs `WHERE <ts> > ?` (no wall-clock upper bound) and advances the watermark to the max
/// timestamp actually read, so no row is dropped to commit latency or writer/NES clock skew — the
/// old (watermark, now] window lost every row whose timestamp landed at/after NES's poll instant.
constexpr std::size_t WINDOW_PARAMETER_COUNT = 1;

/// Chronological comparison of two SQL_TIMESTAMP_STRUCTs (field-lexicographic == time order).
inline bool isAfter(const SQL_TIMESTAMP_STRUCT& a, const SQL_TIMESTAMP_STRUCT& b)
{
    return std::tie(a.year, a.month, a.day, a.hour, a.minute, a.second, a.fraction)
        > std::tie(b.year, b.month, b.day, b.hour, b.minute, b.second, b.fraction);
}

std::string showError(SQLSMALLINT handleType, SQLHANDLE handle, const std::string& context)
{
    std::array<SQLCHAR, SQL_STATE_BUFFER_SIZE> sqlState{};
    std::array<SQLCHAR, SQL_MAX_MESSAGE_LENGTH> message{};
    SQLINTEGER nativeError = 0;
    SQLSMALLINT messageLen = 0;
    SQLSMALLINT recNum = 1;

    std::stringstream errorMessage;
    errorMessage << "Error in " << context << ":\n";
    while (SQLGetDiagRec(
               handleType,
               handle,
               recNum,
               sqlState.data(),
               &nativeError,
               message.data(),
               static_cast<SQLSMALLINT>(message.size()),
               &messageLen)
           == SQL_SUCCESS)
    {
        errorMessage << "  [" << sqlState.data() << "] (" << nativeError << ") " << message.data() << "\n";
        recNum++;
    }

    return errorMessage.str();
}

}

/// A constexpr template can't replicate this: `throw CannotOpenSource(...)` needs the caller's
/// source location for the exception's stack trace; the macro form preserves __FILE__:__LINE__.
/// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define CHECK(rc, handleType, handle, context) \
    do \
    { \
        const SQLRETURN check_rc = (rc); \
        if (check_rc != SQL_SUCCESS && check_rc != SQL_SUCCESS_WITH_INFO) \
        { \
            if (check_rc == SQL_ERROR) \
            { \
                throw CannotOpenSource("{}: {}", (context), showError((handleType), (handle), (context))); \
            } \
            NES_WARNING("{}:{}", (context), showError((handleType), (handle), (context))); \
        } \
    } while (0)

struct SqlStmtDeleter
{
    void operator()(SQLHSTMT handle) const noexcept
    {
        if (handle != SQL_NULL_HSTMT)
        {
            NES_INFO("Freeing STMT");
            SQLFreeHandle(SQL_HANDLE_STMT, handle);
        }
    }
};

using StmtHandle = std::unique_ptr<void, SqlStmtDeleter>;

struct SqlConnectionDeleter
{
    void operator()(SQLHDBC handle) const noexcept
    {
        if (handle != SQL_NULL_HDBC)
        {
            NES_INFO("Delete Connection");
            SQLDisconnect(handle);
            SQLFreeHandle(SQL_HANDLE_DBC, handle);
        }
    }
};

using ConnectionHandle = std::unique_ptr<void, SqlConnectionDeleter>;

struct SqlEnvironmentDeleter
{
    void operator()(SQLHENV handle) const noexcept
    {
        if (handle != SQL_NULL_HENV)
        {
            NES_INFO("Delete Environment");
            SQLFreeHandle(SQL_HANDLE_ENV, handle);
        }
    }
};

using EnvironmentHandle = std::unique_ptr<void, SqlEnvironmentDeleter>;

struct Connection
{
    EnvironmentHandle env;
    ConnectionHandle connection;

    explicit Connection(const ConnectionInformation& connectionInformation)
    {
        /// vcpkg's statically-linked unixODBC does not default its ODBCSYSINI to the system /etc,
        /// so drivers registered there (e.g. msodbcsql18's "ODBC Driver 18 for SQL Server" in
        /// /etc/odbcinst.ini) are invisible and SQLDriverConnect fails with "Can't open lib ... :
        /// file not found". Point it at /etc so the driver manager resolves the DRIVER={..} name.
        /// Matches ODBCLabValSource's ODBCConnection.
        setenv("ODBCSYSINI", "/etc", 1);
        setenv("ODBCINI", "/etc/odbc.ini", 1);

        auto connectionString = buildConnectionString(connectionInformation);

        NES_DEBUG("Connecting to database: {}", connectionString);
        {
            SQLHANDLE tmp = SQL_NULL_HENV;
            const auto rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &tmp);
            CHECK(rc, SQL_HANDLE_ENV, tmp, "SQLAllocHandle ENV");
            this->env = EnvironmentHandle{tmp};
        }

        SQLSetEnvAttr(env.get(), SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0);

        {
            SQLHANDLE tmp = SQL_NULL_HDBC;
            const auto rc = SQLAllocHandle(SQL_HANDLE_DBC, env.get(), &tmp);
            CHECK(rc, SQL_HANDLE_DBC, tmp, "SQLAllocHandle DBC");
            this->connection = ConnectionHandle{tmp};
        }

        std::array<SQLCHAR, OUT_CONNECTION_STRING_SIZE> outConnStr{};
        SQLSMALLINT outConnStrLen = 0;
        const auto rc = SQLDriverConnect(
            connection.get(),
            nullptr,
            reinterpret_cast<SQLCHAR*>(connectionString.data()),
            SQL_NTS,
            outConnStr.data(),
            static_cast<SQLSMALLINT>(outConnStr.size()),
            &outConnStrLen,
            SQL_DRIVER_NOPROMPT);
        CHECK(rc, SQL_HANDLE_DBC, connection.get(), "SQLDriverConnect");
    }
};

struct PreparedStatement
{
    struct ApplyFixup
    {
        std::span<const std::byte> input;
        std::span<std::byte> output;
        FixupFunction function;

        void operator()() const { function(input, output); }
    };

    struct ApplyVarSized
    {
        ColumnIndex columnIndex;
        CType cType;
        std::span<std::byte> output;
    };

    StmtHandle statement;
    std::unique_ptr<SQL_TIMESTAMP_STRUCT> timestampLower;
    /// Byte offset, within fixupTuple, of the watermark column's raw SQL_TIMESTAMP_STRUCT. Set at
    /// bind time when the query has a TIMESTAMP column. After each fetched row the watermark
    /// (timestampLower, the single `?` lower bound) is advanced to the max timestamp read, so the
    /// next poll reads `WHERE <ts> > <max-seen>`.
    size_t watermarkFixupOffset{0};
    bool hasWatermarkColumn{false};
    /// Whole-hour shift applied to the UTC wall clock for the initial watermark, so it matches a
    /// source column stored in a non-UTC timezone (see ConfigParametersODBC).
    std::chrono::hours timezoneOffset{0};
    std::vector<std::byte> outputTuple;
    size_t outputTupleSize{0};
    std::vector<std::byte> fixupTuple;
    std::vector<TupleBuffer> varsizedData;

    std::vector<ApplyFixup> fixupFunctions;
    std::vector<ApplyVarSized> varsizedAccesses;

    static std::vector<SQLType> getSqlResultTypes(SQLHSTMT hStmt)
    {
        SQLSMALLINT numCols = 0;
        const auto ret = SQLNumResultCols(hStmt, &numCols); /// how many columns the SELECT produces
        CHECK(ret, SQL_HANDLE_STMT, hStmt, "SQLNumResultCols");
        std::vector<SQLType> sqlTypes;
        sqlTypes.reserve(numCols);

        for (SQLSMALLINT i = 1; i <= numCols; ++i)
        {
            std::array<SQLCHAR, COLUMN_NAME_BUFFER_SIZE> name{};
            SQLSMALLINT nameLen = 0;
            SQLSMALLINT sqlType = 0;
            SQLULEN colSize = 0;
            SQLSMALLINT decimals = 0;
            SQLSMALLINT nullable = 0;

            const auto rc = SQLDescribeCol(
                hStmt,
                i,
                name.data(),
                static_cast<SQLSMALLINT>(name.size()),
                &nameLen,
                &sqlType, /// SQL_INTEGER, SQL_DOUBLE, SQL_VARCHAR, ...
                &colSize, /// for VARCHAR: max chars; for numerics: precision
                &decimals,
                &nullable); /// SQL_NULLABLE / SQL_NO_NULLS / SQL_NULLABLE_UNKNOWN
            CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLDescribeCol");
            sqlTypes.emplace_back(SQLType{.sqlType = sqlType, .isNullable = (nullable == SQL_NULLABLE)});
        }
        return sqlTypes;
    }

    static std::vector<SQLType> getSqlParameterTypes(SQLHSTMT hStmt)
    {
        SQLSMALLINT numCols = 0;
        const auto ret = SQLNumParams(hStmt, &numCols); /// how many ? statements are in the query
        CHECK(ret, SQL_HANDLE_STMT, hStmt, "SQLNumResultCols");
        std::vector<SQLType> sqlTypes;
        sqlTypes.reserve(numCols);

        for (SQLSMALLINT i = 1; i <= numCols; ++i)
        {
            SQLSMALLINT dataType = 0;
            SQLULEN paramSize = 0;
            SQLSMALLINT decimalDigits = 0;
            SQLSMALLINT nullable = 0;

            const auto rc = SQLDescribeParam(hStmt, i, &dataType, &paramSize, &decimalDigits, &nullable);

            CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLDescribeParam");
            sqlTypes.emplace_back(SQLType{.sqlType = dataType, .isNullable = (nullable == SQL_NULLABLE)});
        }
        return sqlTypes;
    }

    /// Advance the lower watermark to the last fetched row's timestamp if it is newer, so the next
    /// poll reads strictly after the max timestamp seen so far. Reads the raw SQL_TIMESTAMP the
    /// driver wrote into fixupTuple for the watermark column during SQLFetch.
    /// NOLINTNEXTLINE(readability-make-member-function-const): mutates the pointed-to bound parameter.
    void advanceWatermarkFromLastRow()
    {
        if (!hasWatermarkColumn)
        {
            return;
        }
        SQL_TIMESTAMP_STRUCT rowTs{};
        std::memcpy(&rowTs, fixupTuple.data() + watermarkFixupOffset, sizeof(SQL_TIMESTAMP_STRUCT));
        if (isAfter(rowTs, *timestampLower))
        {
            *timestampLower = rowTs;
        }
    }

    static PreparedStatement bind(const Connection& connection, const Schema& schema, std::string query, std::chrono::hours timezoneOffset)
    {
        SQLHSTMT hStmt = SQL_NULL_HSTMT;
        auto rc = SQLAllocHandle(SQL_HANDLE_STMT, connection.connection.get(), &hStmt);
        CHECK(rc, SQL_HANDLE_DBC, connection.connection.get(), "SQLAllocHandle STMT");
        rc = SQLPrepare(hStmt, reinterpret_cast<SQLCHAR*>(query.data()), SQL_NTS);
        CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLPrepare");
        auto statement = StmtHandle{hStmt};

        auto sqlTypes = getSqlResultTypes(hStmt);
        NES_DEBUG("Binding {} to {}.", fmt::join(sqlTypes, ", "), schema);

        if (sqlTypes.size() != schema.getNumberOfFields())
        {
            throw CannotOpenSource("Query returned {} columns, but schema expects {}.", sqlTypes.size(), schema.getNumberOfFields());
        }

        auto columnMappingResults = NES::views::enumerate(std::views::zip(sqlTypes, schema))
            | std::views::transform(
                                        [](const auto& types)
                                        {
                                            auto [index, type] = types;
                                            auto [sqlType, nesType] = type;
                                            return lookupColumnMapping(ColumnIndex(index), nesType.dataType, sqlType);
                                        })
            | std::ranges::to<std::vector<std::expected<ColumnMapping, std::string>>>();

        auto errors = columnMappingResults | std::views::filter([](const auto& result) { return !result.has_value(); })
            | std::views::transform([](const auto& result) -> std::string { return result.error(); })
            | std::ranges::to<std::vector<std::string>>();
        if (!errors.empty())
        {
            throw CannotOpenSource("Could not bind query to schema: \n{}", fmt::join(errors, "\n\t-"));
        }

        auto columnMappings = columnMappingResults | std::views::filter([](const auto& result) { return result.has_value(); })
            | std::views::transform([](const auto& result) { return result.value(); }) | std::ranges::to<std::vector<ColumnMapping>>();
        std::ranges::sort(columnMappings, {}, &ColumnMapping::columnIndex);

        size_t fixupSize = 0;
        const size_t tupleSize = schema.getSizeOfSchemaInBytes();
        for (auto& columnMapping : columnMappings)
        {
            fixupSize += std::visit(
                Overloaded{
                    [&](const Fixup& fixup) { return fixup.fixupSize; },
                    [&](const auto&) { return static_cast<size_t>(0); },
                },
                columnMapping.translation);
        }

        std::vector<std::byte> outputTuple(tupleSize);
        auto outputTupleMemory = std::span{outputTuple};
        std::vector<std::byte> fixupTuple(fixupSize);
        auto fixupTupleMemory = std::span{fixupTuple};

        std::vector<ApplyFixup> applyFixups;
        std::vector<ApplyVarSized> applyVarSized;
        size_t watermarkFixupOffset = 0;
        bool watermarkColumnFound = false;
        for (const auto& columnMapping : columnMappings)
        {
            /// This branch's DataType has no nullability, so every column binds as
            /// non-nullable: no per-field null byte, and a null indicator of nullptr.
            /// A SQL NULL in a fixed-width column then surfaces as an ODBC fetch error
            /// (22002); a NULL in a variable-sized column is coerced to empty in fetch().
            std::visit(
                Overloaded{
                    [&](const Fixup& fixup)
                    {
                        applyFixups.emplace_back(
                            fixupTupleMemory.first(fixup.fixupSize),
                            outputTupleMemory.first(columnMapping.nesType.getSizeInBytes()),
                            fixup.fixupFunction);
                        rc = SQLBindCol(
                            hStmt,
                            columnMapping.columnIndex.index,
                            fixup.cType.type,
                            fixupTupleMemory.data(),
                            static_cast<SQLLEN>(fixup.fixupSize),
                            nullptr);
                        CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindCol");
                        /// The (single) TIMESTAMP column is the watermark column: record where the
                        /// driver writes its raw SQL_TIMESTAMP_STRUCT so the poller can read the row's
                        /// timestamp back and advance the `> ?` lower bound (advanceWatermarkFromLastRow).
                        if (fixup.cType.type == SQL_C_TYPE_TIMESTAMP && !watermarkColumnFound)
                        {
                            watermarkFixupOffset = static_cast<size_t>(fixupTupleMemory.data() - std::span{fixupTuple}.data());
                            watermarkColumnFound = true;
                        }
                        fixupTupleMemory = fixupTupleMemory.subspan(fixup.fixupSize);
                    },
                    [&](const Direct& direct)
                    {
                        rc = SQLBindCol(
                            hStmt,
                            columnMapping.columnIndex.index,
                            direct.cType.type,
                            outputTupleMemory.data(),
                            columnMapping.nesType.getSizeInBytes(),
                            nullptr);
                        CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindCol");
                    },
                    [&](const VarSized& varSized)
                    {
                        applyVarSized.emplace_back(
                            columnMapping.columnIndex, varSized.cType, outputTupleMemory.first(sizeof(VariableSizedAccess)));
                        CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindCol");
                    },
                },
                columnMapping.translation);

            outputTupleMemory = outputTupleMemory.subspan(columnMapping.nesType.getSizeInBytes());
        }


        auto parameterTypes = getSqlParameterTypes(hStmt);
        NES_DEBUG("Binding {} to {}.", fmt::join(parameterTypes, ", "), schema);
        INVARIANT(
            parameterTypes.size() == WINDOW_PARAMETER_COUNT,
            "validateAndFormat enforces exactly {} '?' parameter, but the prepared query reports {}.",
            WINDOW_PARAMETER_COUNT,
            parameterTypes.size());
        /// Single lower bound: start the watermark at "now" so the poller streams rows inserted from
        /// this point on (set to epoch 0 instead to backfill all history). It advances to the max row
        /// timestamp read (advanceWatermarkFromLastRow); there is no upper bound.
        auto timestampLower = std::make_unique<SQL_TIMESTAMP_STRUCT>(convertToTimestamp(std::chrono::system_clock::now() + timezoneOffset));

        /// ColumnSize/DecimalDigits give microsecond precision (YYYY-MM-DD HH:MM:SS.ffffff); the
        /// earlier (16, 0) meant minute precision and truncated the bound parameter against
        /// microsecond-precision `timestamp` columns, so the query returned no rows.
        rc = SQLBindParameter(
            hStmt,
            1,
            SQL_PARAM_INPUT,
            SQL_C_TYPE_TIMESTAMP,
            SQL_TYPE_TIMESTAMP,
            TIMESTAMP_COLUMN_SIZE,
            TIMESTAMP_DECIMAL_DIGITS,
            timestampLower.get(),
            sizeof(SQL_TIMESTAMP_STRUCT),
            nullptr);
        CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindParameter");

        auto preparedStatement = PreparedStatement{
            .statement = std::move(statement),
            .timestampLower = std::move(timestampLower),
            .watermarkFixupOffset = watermarkFixupOffset,
            .hasWatermarkColumn = watermarkColumnFound,
            .timezoneOffset = timezoneOffset,
            .outputTuple = std::move(outputTuple),
            .outputTupleSize = tupleSize,
            .fixupTuple = std::move(fixupTuple),
            .varsizedData = {},
            .fixupFunctions = applyFixups,
            .varsizedAccesses = applyVarSized};
        return preparedStatement;
    }

    void exec()
    {
        NES_DEBUG(
            "ODBCSource poll: fetching rows with timestamp > {} (NES {})",
            getTimestamp(*timestampLower),
            toNESTimestamp(getTimestamp(*timestampLower)));
        const SQLRETURN rc = SQLExecute(statement.get());
        CHECK(rc, SQL_HANDLE_STMT, statement.get(), "SQLExecute");
    }

    bool fetch(TupleBuffer& parent, AbstractBufferProvider& provider)
    {
        const auto rc = SQLFetch(statement.get());
        if (rc == SQL_NO_DATA)
        {
            NES_DEBUG("ODBCSource fetch: SQL_NO_DATA (end of this poll's result set)");
            SQLFreeStmt(statement.get(), SQL_CLOSE);
            return false;
        }
        CHECK(rc, SQL_HANDLE_STMT, statement.get(), "SQLFetch");

        NES_DEBUG("ODBCSource fetch: got a row");
        for (const auto& [index, cType, destination] : varsizedAccesses)
        {
            std::array<SQLCHAR, 1> probe{};
            SQLLEN totalLen = 0;
            SQLRETURN getDataRc
                = SQLGetData(statement.get(), index.index, cType.type, probe.data(), static_cast<SQLLEN>(probe.size()), &totalLen);
            CHECK(getDataRc, SQL_HANDLE_STMT, statement.get(), "SQLGetData");


            if (totalLen == SQL_NULL_DATA || totalLen == 0)
            {
                /// This branch has no nullable columns, so a SQL NULL cannot be
                /// represented distinctly and is coerced to an empty variable-sized
                /// value (same as a genuinely empty string). A zero Size makes the
                /// index a don't-care (no child buffer is read), so index 0 stands in
                /// for the branch's old INVALID_VARIABLE_SIZED_INDEX sentinel, which
                /// our tree's VariableSizedAccess no longer defines. The output tuple
                /// is reused across fetches, so zeroing the slot also prevents a stale
                /// non-zero size from a prior row emitting a bogus child payload.
                const auto emptyAccess = VariableSizedAccess{VariableSizedAccess::Index{0}, VariableSizedAccess::Size{0}};
                std::memcpy(destination.data(), &emptyAccess, sizeof(emptyAccess));
                continue;
            }

            TupleBuffer resultBuffer;
            if (totalLen == SQL_NO_TOTAL)
            {
                /// SQL Server cannot tell the size of the variable sized data, and i dont have the patience to implement that properly. We will allocate what ever fixed size buffer and attempt to fill or truncate the variable sized data.
                resultBuffer = provider.getBufferBlocking();
                totalLen = static_cast<SQLLEN>(resultBuffer.getBufferSize());
            }
            else
            {
                INVARIANT(totalLen >= 0, "SQLGetData returned a negative length: {}.", totalLen);
                /// Account for zero termination
                resultBuffer = provider.getUnpooledBuffer(totalLen + 1).value();
            }

            SQLLEN bytesRead = 0;
            getDataRc = SQLGetData(
                statement.get(),
                index.index,
                cType.type,
                resultBuffer.getAvailableMemoryArea<std::byte>().data(),
                totalLen + 1,
                &bytesRead);


            CHECK(getDataRc, SQL_HANDLE_STMT, statement.get(), "SQLGetData");
            auto childBufferIndex = parent.storeChildBuffer(resultBuffer);

            NES_DEBUG("SQLGetData returned {} bytes.", bytesRead);
            const std::string_view odbcVarSized{resultBuffer.getAvailableMemoryArea<char>().data(), static_cast<size_t>(bytesRead)};
            // NES_DEBUG("Received varsized: {}", odbcVarSized);
            const auto access = VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size{static_cast<uint64_t>(bytesRead)}};
            std::memcpy(destination.data(), &access, sizeof(access));
        }

        for (const auto& fixup : fixupFunctions)
        {
            fixup();
        }

        /// Advance the `> ?` lower bound to this row's timestamp (the max, since rows are read ASC),
        /// so the next poll continues strictly after it. Keyed to the data, not the wall clock, so
        /// no row is lost to commit latency or writer/NES clock skew.
        advanceWatermarkFromLastRow();
        if (hasWatermarkColumn)
        {
            SQL_TIMESTAMP_STRUCT rowTs{};
            std::memcpy(&rowTs, fixupTuple.data() + watermarkFixupOffset, sizeof(SQL_TIMESTAMP_STRUCT));
            NES_DEBUG("ODBCSource fetch: row timestamp {}, watermark now {}", getTimestamp(rowTs), getTimestamp(*timestampLower));
        }

        NES_DEBUG(
            "ODBCSource fetch: fetched tuple ({} bytes): {:02x}",
            outputTupleSize,
            fmt::join(
                std::span{outputTuple.data(), outputTupleSize}
                    | std::views::transform([](const std::byte byte) { return std::to_integer<unsigned>(byte); }),
                ""));

        return true;
    }

    [[nodiscard]] std::span<const std::byte> data() const { return {outputTuple.data(), outputTupleSize}; }
};

struct ConnectingToDatabaseState

{
};

struct FetchFromDatabaseState
{
    Connection connection;
    PreparedStatement statement;
};

struct FetchFromBufferState
{
    Connection connection;
    PreparedStatement statement;
    Timestamp watermark;
};

using SourceState = std::variant<ConnectingToDatabaseState, FetchFromDatabaseState, FetchFromBufferState>;

struct Context
{
    ConnectionInformation connectionInformation;
    SourceState state;
};

ODBCSource::ODBCSource(const SourceDescriptor& sourceDescriptor)
    : pollIntervalMs(sourceDescriptor.getFromConfig(ConfigParametersODBC::POLL_INTERVAL_MS))
    , query(sourceDescriptor.getFromConfig(ConfigParametersODBC::QUERY))
    , schema(*sourceDescriptor.getLogicalSource().getSchema())
    , timezoneOffset(std::chrono::hours(sourceDescriptor.getFromConfig(ConfigParametersODBC::TIMEZONE_OFFSET_HOURS)))
    , sourceContext(std::make_unique<Context>(
          ConnectionInformation{
              .server = sourceDescriptor.getFromConfig(ConfigParametersODBC::DB_HOST),
              .port = sourceDescriptor.getFromConfig(ConfigParametersODBC::DB_PORT),
              .database = sourceDescriptor.getFromConfig(ConfigParametersODBC::DATABASE),
              .username = sourceDescriptor.getFromConfig(ConfigParametersODBC::USERNAME),
              .password = sourceDescriptor.getFromConfig(ConfigParametersODBC::PASSWORD),
              .driver = sourceDescriptor.getFromConfig(ConfigParametersODBC::DRIVER),
              .trustServerCertificate = sourceDescriptor.getFromConfig(ConfigParametersODBC::TRUST_SERVER_CERTIFICATE)},
          ConnectingToDatabaseState{}))
{
}

std::ostream& ODBCSource::toString(std::ostream& str) const
{
    str << fmt::format("\nODBCSource(\n  pollIntervalMs: {}\n  query: {}\n  schema: {}\n)\n", pollIntervalMs, query, schema);
    return str;
}

ODBCSource::~ODBCSource() = default;

void ODBCSource::open(std::shared_ptr<AbstractBufferProvider> bufferProviderPtr)
{
    this->bufferProvider = std::move(bufferProviderPtr);
    /// Connect + bind eagerly here so an unreachable endpoint or an
    /// unbindable schema fails fast in open() rather than mid-ingest: the
    /// conformance battery's bad_endpoint scenario expects an open-phase
    /// connector error, and the harness's READY/GO handshake only sends GO
    /// *after* open() returns — a connect deferred to fillTupleBuffer would
    /// otherwise block waiting for a GO that never comes. The query EXEC
    /// stays deferred to fillTupleBuffer (FetchFromDatabaseState), so a
    /// round_trip seed committed after READY is still visible to the first
    /// poll's (watermark, now] window.
    Connection connection(sourceContext->connectionInformation);
    auto statement = PreparedStatement::bind(connection, schema, query, timezoneOffset);
    sourceContext->state = FetchFromDatabaseState{.connection = std::move(connection), .statement = std::move(statement)};
    NES_DEBUG("ODBCSource connected successfully.");
}

namespace
{
bool waitForTimeout(std::chrono::milliseconds timeout, const std::stop_token& stoken)
{
    std::mutex mtx;
    std::condition_variable_any cv;
    std::unique_lock lock(mtx);
    cv.wait_for(lock, stoken, timeout, [&stoken] { return stoken.stop_requested(); });
    return !stoken.stop_requested();
}
}

Source::FillTupleBufferResult ODBCSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stoken)
{
    auto currentBuffer = tupleBuffer.getAvailableMemoryArea<std::byte>();
    size_t numberOfTuples = 0;
    bool shouldReturn = false;
    auto watermark = Timestamp(0);

    while (!shouldReturn && !stoken.stop_requested())
    {
        sourceContext->state = std::visit(
            Overloaded{
                /// NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved): the variant's content is replaced via the returned state value; the visitor binds the source rvalue but does not move from the empty ConnectingToDatabaseState.
                [this](ConnectingToDatabaseState&&) -> SourceState
                {
                    Connection connection(sourceContext->connectionInformation);
                    auto statement = PreparedStatement::bind(connection, schema, query, timezoneOffset);
                    return FetchFromDatabaseState{.connection = std::move(connection), .statement = std::move(statement)};
                },
                /// NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved): moved-from at the matching return sites (`FetchFromBufferState{.connection = std::move(state.connection), …}`).
                [&](FetchFromDatabaseState&& state) -> SourceState
                {
                    if (!waitForTimeout(std::chrono::milliseconds(pollIntervalMs), stoken))
                    {
                        NES_DEBUG("ODBCSource was requested to stop");
                        return state;
                    }

                    state.statement.exec();
                    const auto timestamp = *state.statement.timestampLower;
                    return FetchFromBufferState{
                        .connection = std::move(state.connection),
                        .statement = std::move(state.statement),
                        .watermark = Timestamp(toNESTimestamp(getTimestamp(timestamp)))};
                },
                [&](FetchFromBufferState&& state) -> SourceState
                {
                    if (currentBuffer.size() < state.statement.data().size())
                    {
                        watermark = state.watermark;
                        shouldReturn = true;
                        return FetchFromBufferState{
                            .connection = std::move(state.connection),
                            .statement = std::move(state.statement),
                            .watermark = std::move(watermark)};
                    }

                    const auto hasTuple = state.statement.fetch(tupleBuffer, *bufferProvider);
                    if (!hasTuple)
                    {
                        watermark = Timestamp(toNESTimestamp(getTimestamp(*state.statement.timestampLower)));
                        shouldReturn = true;
                        return FetchFromDatabaseState{.connection = std::move(state.connection), .statement = std::move(state.statement)};
                    }

                    {
                        std::string hex;
                        for (const auto b : state.statement.data())
                        {
                            hex += fmt::format("{:02x}", std::to_integer<unsigned>(b));
                        }
                        NES_DEBUG(
                            "ODBCSource writing tuple #{} to buffer ({} bytes): {}", numberOfTuples, state.statement.data().size(), hex);
                    }
                    std::ranges::copy(state.statement.data(), currentBuffer.begin());
                    currentBuffer = currentBuffer.subspan(state.statement.data().size());
                    numberOfTuples += 1;
                    return FetchFromBufferState{std::move(state)};
                }},
            std::move(sourceContext->state));
    }

    NES_DEBUG("ODBCSource emitting buffer: {} tuple(s), watermark {}", numberOfTuples, watermark);
    tupleBuffer.setWatermark(watermark);

    return FillTupleBufferResult::withBytes(numberOfTuples);
}

DescriptorConfig::Config ODBCSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    /// Probe DRIVER's presence eagerly so a missing key throws std::out_of_range here
    /// (the validateAndFormat path treats DRIVER as required for the connection string).
    (void)config.at(ConfigParametersODBC::DRIVER);

    /// The incremental poller binds exactly one `?` parameter (the lower watermark bound),
    /// so reject any other count here rather than at connect time.
    if (const auto query = config.find(ConfigParametersODBC::QUERY); query != config.end())
    {
        if (const auto numParameters = std::ranges::count(query->second, '?'); std::cmp_not_equal(numParameters, WINDOW_PARAMETER_COUNT))
        {
            throw InvalidConfigParameter(
                "ODBC source query must have exactly {} '?' parameter for the `> watermark` lower bound, but has {}.",
                WINDOW_PARAMETER_COUNT,
                numParameters);
        }
    }

    /// The timezone offset only shifts the window's wall-clock bounds by whole hours; a value beyond a
    /// full day's rotation is a configuration mistake. Reject it here (a non-integer string is left to
    /// the type-checked validateAndFormat below to report).
    if (const auto it = config.find(ConfigParametersODBC::TIMEZONE_OFFSET_HOURS); it != config.end())
    {
        int32_t offset = 0;
        const auto* const end = it->second.data() + it->second.size();
        const auto [ptr, ec] = std::from_chars(it->second.data(), end, offset);
        if (ec == std::errc{} && ptr == end && (offset < -24 || offset > 24))
        {
            throw InvalidConfigParameter("ODBC source timezone_offset_hours must be within [-24, 24], but is {}.", offset);
        }
    }

    return DescriptorConfig::validateAndFormat<ConfigParametersODBC>(std::move(config), NAME);
}

void ODBCSource::close()
{
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): the registry's generated *.inc declares this entry point by value; switching to const& breaks linkage.
SourceValidationRegistryReturnType RegisterODBCSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return ODBCSource::validateAndFormat(sourceConfig.config);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): the registry's generated *.inc declares this entry point by value; switching to const& breaks linkage.
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterODBCSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<ODBCSource>(sourceRegistryArguments.sourceDescriptor);
}
}

/// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast,readability-identifier-length)
