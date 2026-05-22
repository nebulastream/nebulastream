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

/// Test data loader for the ODBC source plugin. Owns the InlineData / FileData
/// registrars the `.test` runner invokes for `ATTACH INLINE` / `ATTACH FILE`,
/// and seeds those rows into the PostgreSQL table the source then reads.
///
/// Keeping this in its own translation unit leaves ODBCSource.{hpp,cpp} a pure
/// runtime reader with no test-scaffold awareness — the same split the MQTT
/// source uses with MQTTTestPublisher.cpp.
///
/// The external_systest dispatcher (ExternalSystestDispatch.cpp) brings up a
/// PostgreSQL container and exports `NES_EXTERNAL_HOST` / `NES_EXTERNAL_PORT_DATABASE`
/// for the host and port it allocated. `applyDispatchOverrides` rewrites the
/// descriptor's `db_host` / `db_port` from those — exactly as the MQTT loader
/// rewrites its `serveruri`. The registrar then opens its own short-lived ODBC
/// connection, INSERTs the ATTACH rows into `sync_table` in one transaction,
/// and disconnects. Seeding runs synchronously at descriptor-construction time,
/// before the query executes, so the source's first poll already sees a fully
/// populated table and EOS-es once it stops yielding new rows.
///
/// `init-db.sql` in the profile dir creates the empty table on first boot; only
/// the row data lives in the `.test` file. Seeding over a live connection (vs a
/// generated pre-boot SQL script) is the mechanism every comparable system —
/// Flink, Spark, ClickHouse, Materialize — uses; the `docker compose up --wait`
/// + `pg_isready` healthcheck is the barrier that makes it race-free.

#include <array>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

#include <Sources/SourceDataProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <ODBCSource.hpp>

/// The ODBC C API takes non-const `SQLCHAR*` even for input-only strings and
/// reports diagnostics into unsigned-char buffers. Bridging NebulaStream's
/// `std::string` / `char` storage to it unavoidably requires reinterpret_cast
/// across the char signedness. The check is silenced file-wide for that C-API
/// boundary; every cast here is mandated by the unixODBC headers.
/// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast)
namespace NES
{

namespace
{

/// Rewrite the descriptor's `db_host` / `db_port` to the endpoint the
/// external_systest dispatcher allocated for the PostgreSQL container. On a run
/// without the dispatcher the vars are unset and the `.test` file's symbolic
/// placeholders stand — but a `# requires: odbc` test is only ever run through
/// the dispatcher, so that path is not a supported configuration.
void applyDispatchOverrides(PhysicalSourceConfig& cfg)
{
    /// NOLINTNEXTLINE(concurrency-mt-unsafe): called from registry-time descriptor construction, before any worker thread runs.
    if (const char* host = std::getenv("NES_EXTERNAL_HOST"))
    {
        cfg.sourceConfig[ConfigParametersODBC::HOST] = host;
    }
    /// NOLINTNEXTLINE(concurrency-mt-unsafe): see above.
    if (const char* port = std::getenv("NES_EXTERNAL_PORT_DATABASE"))
    {
        cfg.sourceConfig[ConfigParametersODBC::PORT] = port;
    }
}

/// ODBC handles for the short-lived seeding connection, freed in reverse order
/// on scope exit. `checkError` in ODBCConnection.cpp is file-local, so the
/// loader carries its own teardown rather than reaching for the source's class.
struct SeedConnection
{
    SQLHENV henv = SQL_NULL_HENV;
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLHSTMT hstmt = SQL_NULL_HSTMT;
    bool connected = false;

    SeedConnection() = default;
    SeedConnection(const SeedConnection&) = delete;
    SeedConnection& operator=(const SeedConnection&) = delete;
    SeedConnection(SeedConnection&&) = delete;
    SeedConnection& operator=(SeedConnection&&) = delete;

    ~SeedConnection()
    {
        if (hstmt != SQL_NULL_HSTMT)
        {
            SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        }
        if (hdbc != SQL_NULL_HDBC)
        {
            if (connected)
            {
                SQLDisconnect(hdbc);
            }
            SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        }
        if (henv != SQL_NULL_HENV)
        {
            SQLFreeHandle(SQL_HANDLE_ENV, henv);
        }
    }
};

/// Length of an ODBC SQLSTATE buffer: five status characters plus a null terminator.
constexpr std::size_t sqlStateBufferSize = 6;

/// Throw with every ODBC diagnostic record attached, so a failed connect or
/// INSERT surfaces the PostgreSQL message rather than a bare return code.
void throwOnOdbcError(const SQLRETURN ret, const SQLSMALLINT handleType, SQLHANDLE handle, const std::string_view operation)
{
    if (SQL_SUCCEEDED(ret))
    {
        return;
    }
    std::string diagnostics;
    SQLSMALLINT recNum = 1;
    std::array<SQLCHAR, sqlStateBufferSize> sqlState = {};
    std::array<SQLCHAR, SQL_MAX_MESSAGE_LENGTH> message = {};
    SQLINTEGER nativeError = 0;
    SQLSMALLINT textLength = 0;
    while (SQLGetDiagRec(
               handleType,
               handle,
               recNum,
               sqlState.data(),
               &nativeError,
               message.data(),
               static_cast<SQLSMALLINT>(message.size()),
               &textLength)
           == SQL_SUCCESS)
    {
        diagnostics += fmt::format(
            "\n  [{}] {} (native error {})",
            reinterpret_cast<const char*>(sqlState.data()),
            reinterpret_cast<const char*>(message.data()),
            nativeError);
        ++recNum;
    }
    throw CannotOpenSource("ODBC test loader: {} failed:{}", operation, diagnostics);
}

/// Drop a trailing CR/LF so a row read from a file (or a CRLF-terminated inline
/// block) does not carry the line ending into the last column value.
std::string_view stripLineEnding(std::string_view row)
{
    while (!row.empty() && (row.back() == '\n' || row.back() == '\r'))
    {
        row.remove_suffix(1);
    }
    return row;
}

/// Build `INSERT INTO <table> VALUES ('v0','v1',...)` from one comma-separated
/// row. Values are quoted unconditionally; PostgreSQL coerces the string
/// literals to the column types declared by init-db.sql, so the loader needs no
/// schema of its own. A `'` inside a value is doubled per SQL escaping.
std::string buildInsert(const std::string_view table, const std::string_view row)
{
    std::string values;
    std::size_t pos = 0;
    while (true)
    {
        const auto comma = row.find(',', pos);
        const auto field = row.substr(pos, comma == std::string_view::npos ? std::string_view::npos : comma - pos);
        if (!values.empty())
        {
            values.push_back(',');
        }
        values.push_back('\'');
        for (const char character : field)
        {
            values.push_back(character);
            if (character == '\'')
            {
                values.push_back('\'');
            }
        }
        values.push_back('\'');
        if (comma == std::string_view::npos)
        {
            break;
        }
        pos = comma + 1;
    }
    return fmt::format("INSERT INTO {} VALUES ({});", table, values);
}

/// Open a short-lived ODBC connection to the (dispatcher-allocated) PostgreSQL
/// endpoint and INSERT every row into `sync_table` in a single transaction, so
/// the source's first poll sees either all rows or none.
void seedTable(const PhysicalSourceConfig& cfg, const std::vector<std::string>& rows)
{
    const auto require = [&](const std::string& key) -> const std::string&
    {
        const auto it = cfg.sourceConfig.find(key);
        if (it == cfg.sourceConfig.end())
        {
            throw InvalidConfigParameter("ODBC test loader: `{}` is required to seed the database", key);
        }
        return it->second;
    };
    /// Same connection-string layout as ODBCSource::open, so the loader reaches
    /// exactly the database the source will read.
    auto connectionString = fmt::format(
        "DRIVER={{{}}};SERVER={};PORT={};DATABASE={};UID={};PWD={};",
        require(ConfigParametersODBC::DRIVER),
        require(ConfigParametersODBC::HOST),
        require(ConfigParametersODBC::PORT),
        require(ConfigParametersODBC::DATABASE),
        require(ConfigParametersODBC::USERNAME),
        require(ConfigParametersODBC::PASSWORD));
    const auto& table = require(ConfigParametersODBC::SYNC_TABLE);

    SeedConnection conn;
    throwOnOdbcError(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &conn.henv), SQL_HANDLE_ENV, conn.henv, "allocate environment handle");
    throwOnOdbcError(
        SQLSetEnvAttr(conn.henv, SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0),
        SQL_HANDLE_ENV,
        conn.henv,
        "set ODBC version");
    throwOnOdbcError(SQLAllocHandle(SQL_HANDLE_DBC, conn.henv, &conn.hdbc), SQL_HANDLE_ENV, conn.henv, "allocate connection handle");

    throwOnOdbcError(
        SQLDriverConnect(
            conn.hdbc, nullptr, reinterpret_cast<SQLCHAR*>(connectionString.data()), SQL_NTS, nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT),
        SQL_HANDLE_DBC,
        conn.hdbc,
        "connect to the database to seed it");
    conn.connected = true;

    /// One transaction: a failed INSERT leaves the table untouched rather than
    /// half-seeded, so the source never observes a partially populated table.
    throwOnOdbcError(
        SQLSetConnectAttr(conn.hdbc, SQL_ATTR_AUTOCOMMIT, reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_OFF), 0),
        SQL_HANDLE_DBC,
        conn.hdbc,
        "disable autocommit");
    throwOnOdbcError(SQLAllocHandle(SQL_HANDLE_STMT, conn.hdbc, &conn.hstmt), SQL_HANDLE_DBC, conn.hdbc, "allocate statement handle");

    std::size_t seeded = 0;
    for (const auto& row : rows)
    {
        const auto trimmed = stripLineEnding(row);
        if (trimmed.empty())
        {
            continue;
        }
        auto statement = buildInsert(table, trimmed);
        throwOnOdbcError(
            SQLExecDirect(conn.hstmt, reinterpret_cast<SQLCHAR*>(statement.data()), SQL_NTS), SQL_HANDLE_STMT, conn.hstmt, statement);
        ++seeded;
    }

    throwOnOdbcError(SQLEndTran(SQL_HANDLE_DBC, conn.hdbc, SQL_COMMIT), SQL_HANDLE_DBC, conn.hdbc, "commit the seed transaction");
    NES_DEBUG("ODBC test loader: seeded {} row(s) into `{}`", seeded, table);
}

}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterODBCInlineData(InlineDataRegistryArguments args)
{
    applyDispatchOverrides(args.physicalSourceConfig);
    seedTable(args.physicalSourceConfig, args.tuples);
    return args.physicalSourceConfig;
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterODBCFileData(FileDataRegistryArguments args)
{
    if (!std::filesystem::is_regular_file(args.testFilePath))
    {
        throw CannotOpenSource("ODBC FileData: not a regular file: {}", args.testFilePath.string());
    }
    std::ifstream input(args.testFilePath);
    if (!input.is_open())
    {
        throw CannotOpenSource("ODBC FileData: cannot open {}", args.testFilePath.string());
    }
    std::vector<std::string> rows;
    std::string line;
    while (std::getline(input, line))
    {
        rows.push_back(line);
    }
    applyDispatchOverrides(args.physicalSourceConfig);
    seedTable(args.physicalSourceConfig, rows);
    return args.physicalSourceConfig;
}

}

/// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)
