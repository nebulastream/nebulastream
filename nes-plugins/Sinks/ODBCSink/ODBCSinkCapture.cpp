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

/// Test capture for the ODBC sink plugin -- the sink-side mirror of the ODBC
/// source's ATTACH-time test loader (ODBCTestLoader.cpp). Owns the SinkCapture
/// registrar the systest binder invokes for a `SELECT ... INTO ODBC(...)`
/// query.
///
/// At bind time the registrar rewrites the sink's `db_host` / `db_port` to the
/// dispatcher-allocated endpoint, then opens a read connection to the same
/// database.
///
/// Unlike the MQTT capture, an ODBC sink is *pull-based*: it writes rows into a
/// table, so capturing means reading that table back -- and a read taken while
/// the query is still running races the sink's INSERTs. So the registrar does
/// not read here; it hands the systest runner a result finalizer
/// (SinkCaptureRegistryArguments::resultFinalizer). The runner invokes it
/// exactly once, after the query has reached Stopped and before the result
/// check opens the result file. By then the sink has run every INSERT and
/// disconnected (ODBCSink::stop), and autocommit was on throughout, so a
/// single `SELECT * FROM <table>` is guaranteed to observe the complete
/// output. The finalizer rewrites the result file with the binder-written
/// schema header followed by every row; the result comparison sorts both
/// sides (SystestResultCheck.cpp), so the read-back needs no `ORDER BY`.
///
/// This replaces an earlier eager-polling capture thread. With no post-query
/// hook to synchronise against, that thread could only sample the table on a
/// timer and frequently lost the race against `checkResult`, reading a
/// partially populated table. The finalizer closes that gap: the runner now
/// provides exactly the post-query / pre-`checkResult` hook the read needs.
/// The ODBCSink stays completely test-unaware -- all scaffolding lives here.

#include <array>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <ios>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <ODBCSink.hpp>
#include <SinkCaptureRegistry.hpp>

/// The ODBC C API speaks in `SQLCHAR*` (an unsigned-char alias) even for
/// input-only strings. Bridging NebulaStream's `std::string` / `char` buffers
/// to it unavoidably requires reinterpret_cast across the char signedness. The
/// check is silenced file-wide for that C-API boundary; every cast here is
/// mandated by the unixODBC headers.
/// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast)
namespace NES
{

namespace
{

/// The in-binary dispatcher (ExternalSystestDispatch.cpp) exports
/// NES_EXTERNAL_HOST and NES_EXTERNAL_PORT_DATABASE when it brings the
/// PostgreSQL container up on a host port. Rewrite the sink's db_host / db_port
/// to them so the ODBCSink under test and this capture reader both reach the
/// dispatcher-managed endpoint rather than the symbolic host the .test
/// declares -- the sink-side mirror of ODBCTestLoader::applyDispatchOverrides.
void applyDispatchOverrides(std::unordered_map<std::string, std::string>& cfg)
{
    /// NOLINTNEXTLINE(concurrency-mt-unsafe): called from registry-time descriptor construction, before any worker thread runs.
    if (const char* host = std::getenv("NES_EXTERNAL_HOST"))
    {
        cfg[ConfigParametersODBCSink::HOST] = host;
    }
    /// NOLINTNEXTLINE(concurrency-mt-unsafe): see above.
    if (const char* port = std::getenv("NES_EXTERNAL_PORT_DATABASE"))
    {
        cfg[ConfigParametersODBCSink::PORT] = port;
    }
}

std::string require(const std::unordered_map<std::string, std::string>& cfg, const std::string& key)
{
    const auto it = cfg.find(key);
    if (it == cfg.end())
    {
        throw InvalidConfigParameter("ODBC sink capture: `{}` is required", key);
    }
    return it->second;
}

/// Buffer length for a SQLSTATE code: five characters plus a null terminator.
constexpr std::size_t sqlStateBufferSize = 6;

/// Collect every ODBC diagnostic record of a failed call into one string;
/// nullopt on success. See the twin helper in ODBCSink.cpp.
std::optional<std::string> odbcDiagnostics(const SQLRETURN ret, const SQLSMALLINT handleType, SQLHANDLE handle)
{
    if (SQL_SUCCEEDED(ret))
    {
        return std::nullopt;
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
    return diagnostics;
}

/// ODBC handles for the capture's read connection, freed in reverse order on
/// scope exit. The capture carries its own connection management for the same
/// reason ODBCTestLoader does: ODBCConnection's helpers are file-local.
struct CaptureConnection
{
    SQLHENV henv = SQL_NULL_HENV;
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLHSTMT hstmt = SQL_NULL_HSTMT;
    bool connected = false;

    CaptureConnection() = default;
    CaptureConnection(const CaptureConnection&) = delete;
    CaptureConnection& operator=(const CaptureConnection&) = delete;
    CaptureConnection(CaptureConnection&&) = delete;
    CaptureConnection& operator=(CaptureConnection&&) = delete;

    ~CaptureConnection()
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

/// Open a read connection to the (dispatcher-allocated) database endpoint,
/// using the same connection-string layout as ODBCSink::start. Throws
/// CannotOpenSink with the ODBC diagnostics on any failure.
std::unique_ptr<CaptureConnection> connect(const std::unordered_map<std::string, std::string>& cfg)
{
    auto conn = std::make_unique<CaptureConnection>();
    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &conn->henv) != SQL_SUCCESS)
    {
        throw CannotOpenSink("ODBC sink capture: failed to allocate ODBC environment handle");
    }
    if (const auto diag = odbcDiagnostics(
            SQLSetEnvAttr(conn->henv, SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0), SQL_HANDLE_ENV, conn->henv))
    {
        throw CannotOpenSink("ODBC sink capture: failed to set ODBC version:{}", *diag);
    }
    if (const auto diag = odbcDiagnostics(SQLAllocHandle(SQL_HANDLE_DBC, conn->henv, &conn->hdbc), SQL_HANDLE_ENV, conn->henv))
    {
        throw CannotOpenSink("ODBC sink capture: failed to allocate connection handle:{}", *diag);
    }

    auto connectionString = fmt::format(
        "DRIVER={{{}}};SERVER={};PORT={};DATABASE={};UID={};PWD={};",
        require(cfg, ConfigParametersODBCSink::DRIVER),
        require(cfg, ConfigParametersODBCSink::HOST),
        require(cfg, ConfigParametersODBCSink::PORT),
        require(cfg, ConfigParametersODBCSink::DATABASE),
        require(cfg, ConfigParametersODBCSink::USERNAME),
        require(cfg, ConfigParametersODBCSink::PASSWORD));
    if (const auto diag = odbcDiagnostics(
            SQLDriverConnect(
                conn->hdbc,
                nullptr,
                reinterpret_cast<SQLCHAR*>(connectionString.data()),
                SQL_NTS,
                nullptr,
                0,
                nullptr,
                SQL_DRIVER_NOPROMPT),
            SQL_HANDLE_DBC,
            conn->hdbc))
    {
        throw CannotOpenSink("ODBC sink capture: failed to connect to the database:{}", *diag);
    }
    conn->connected = true;
    if (const auto diag = odbcDiagnostics(SQLAllocHandle(SQL_HANDLE_STMT, conn->hdbc, &conn->hstmt), SQL_HANDLE_DBC, conn->hdbc))
    {
        throw CannotOpenSink("ODBC sink capture: failed to allocate statement handle:{}", *diag);
    }
    return conn;
}

/// Read the whole table back as comma-separated rows: every column is fetched
/// as text (SQL_C_CHAR), so the database does the value formatting and the
/// capture needs no type mapping. Order is irrelevant -- the result check
/// sorts both sides.
std::vector<std::string> readTableRows(CaptureConnection& conn, const std::string& table)
{
    auto query = fmt::format("SELECT * FROM {};", table);
    if (const auto diag
        = odbcDiagnostics(SQLExecDirect(conn.hstmt, reinterpret_cast<SQLCHAR*>(query.data()), SQL_NTS), SQL_HANDLE_STMT, conn.hstmt))
    {
        throw RunningRoutineFailure("ODBC sink capture: SELECT from `{}` failed:{}", table, *diag);
    }

    SQLSMALLINT numColumns = 0;
    SQLNumResultCols(conn.hstmt, &numColumns);

    std::vector<std::string> rows;
    while (SQLFetch(conn.hstmt) == SQL_SUCCESS)
    {
        std::string row;
        for (SQLSMALLINT column = 1; column <= numColumns; ++column)
        {
            /// Test data is small; a fixed buffer avoids the truncation-retry
            /// loop a general-purpose reader would need.
            constexpr std::size_t columnValueBufferSize = 8192;
            std::array<char, columnValueBufferSize> value{};
            SQLLEN indicator = 0;
            const auto ret = SQLGetData(conn.hstmt, column, SQL_C_CHAR, value.data(), value.size(), &indicator);
            if (const auto diag = odbcDiagnostics(ret, SQL_HANDLE_STMT, conn.hstmt))
            {
                throw RunningRoutineFailure("ODBC sink capture: reading column {} of `{}` failed:{}", column, table, *diag);
            }
            if (column > 1)
            {
                row.push_back(',');
            }
            if (indicator != SQL_NULL_DATA)
            {
                row.append(value.data());
            }
        }
        rows.push_back(std::move(row));
    }
    SQLCloseCursor(conn.hstmt);
    return rows;
}

/// Rewrite the systest result file with the binder-written schema header
/// followed by every captured row. Skips the write entirely when the table is
/// empty -- the header-only file the binder already wrote then stands as the
/// (empty) result. The header is read back from that same file; the binder
/// emits it during binding, before the query produces any output.
///
/// The new content goes to a sibling temp file that is then renamed over the
/// result file: rename is atomic, so a partially written file is never
/// observable even if the read is interrupted.
void writeResultFile(const std::filesystem::path& resultFile, const std::vector<std::string>& rows)
{
    if (rows.empty())
    {
        return;
    }
    std::string header;
    {
        std::ifstream input(resultFile);
        std::getline(input, header);
    }
    if (header.empty())
    {
        return;
    }
    const std::filesystem::path tmp = resultFile.string() + ".capture-tmp";
    {
        std::ofstream out(tmp, std::ios::trunc);
        out << header << '\n';
        for (const auto& row : rows)
        {
            out << row << '\n';
        }
    }
    std::filesystem::rename(tmp, resultFile);
}

}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): registry signature fixed by framework.
SinkCaptureRegistryReturnType SinkCaptureGeneratedRegistrar::RegisterODBCSinkCapture(SinkCaptureRegistryArguments args)
{
    applyDispatchOverrides(args.sinkConfig);
    const auto table = require(args.sinkConfig, ConfigParametersODBCSink::TABLE);
    std::shared_ptr<CaptureConnection> conn = connect(args.sinkConfig);

    /// Hand the runner a finalizer instead of a polling thread. It runs once,
    /// after the query has Stopped (so every INSERT is committed) and before
    /// `checkResult` -- see SinkCaptureRegistryArguments::resultFinalizer. No
    /// exception may escape into the runner: a failed read is logged and leaves
    /// the result file header-only, which the result check reports as a
    /// mismatch. The connection is kept alive by this closure and freed with it.
    /// The body catches every std::exception; the only residual throw clang-tidy
    /// can see is the logging call inside the handler, which in practice does not throw.
    /// NOLINTNEXTLINE(bugprone-exception-escape)
    *args.resultFinalizer = [conn = std::move(conn), table, resultFile = args.resultFile]
    {
        try
        {
            writeResultFile(resultFile, readTableRows(*conn, table));
        }
        catch (const std::exception& e)
        {
            NES_ERROR("ODBC sink capture: final table read failed: {}", e.what());
        }
    };

    return args.sinkConfig;
}

}

/// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)
