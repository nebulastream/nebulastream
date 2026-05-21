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

#include <ODBCSink.hpp>

#include <array>
#include <cstddef>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

namespace
{

/// Buffer length for a SQLSTATE code: five characters plus a null terminator.
constexpr std::size_t sqlStateBufferSize = 6;

/// Collect every ODBC diagnostic record attached to a failed call into one
/// string. Returns nullopt on success, so the caller decides which exception
/// type fits the failing operation. `checkError` in ODBCConnection.cpp is
/// file-local, so this sink carries its own — the same split the ODBC test
/// loader makes (see ODBCTestLoader.cpp).
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

/// Build `INSERT INTO <table> VALUES ('v0','v1',...)` from one comma-separated
/// row of the formatted (CSV) output. Values are quoted unconditionally; the
/// target database coerces the string literals to the column types, so the
/// sink needs no schema of its own. A `'` inside a value is doubled per SQL
/// escaping. Embedded commas are not supported — the same naive split the ODBC
/// source's test loader uses (ODBCTestLoader.cpp::buildInsert).
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
        for (const char ch : field)
        {
            values.push_back(ch);
            if (ch == '\'')
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

/// Drop a trailing CR/LF so a row carved out of the formatted buffer does not
/// carry the line ending into the last column value.
std::string_view stripLineEnding(std::string_view row)
{
    while (!row.empty() && (row.back() == '\n' || row.back() == '\r'))
    {
        row.remove_suffix(1);
    }
    return row;
}

}

ODBCSink::ODBCSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , host(sinkDescriptor.getFromConfig(ConfigParametersODBCSink::HOST))
    , port(sinkDescriptor.getFromConfig(ConfigParametersODBCSink::PORT))
    , database(sinkDescriptor.getFromConfig(ConfigParametersODBCSink::DATABASE))
    , username(sinkDescriptor.getFromConfig(ConfigParametersODBCSink::USERNAME))
    , password(sinkDescriptor.getFromConfig(ConfigParametersODBCSink::PASSWORD))
    , driver(sinkDescriptor.getFromConfig(ConfigParametersODBCSink::DRIVER))
    , table(sinkDescriptor.getFromConfig(ConfigParametersODBCSink::TABLE))
{
}

ODBCSink::~ODBCSink()
{
    closeConnection();
}

std::ostream& ODBCSink::toString(std::ostream& os) const
{
    os << "\nODBCSink(";
    os << "\n  host: " << host;
    os << "\n  port: " << port;
    os << "\n  database: " << database;
    os << "\n  table: " << table;
    os << "\n  insertedRows: " << insertedRows;
    os << ")\n";
    return os;
}

void ODBCSink::closeConnection() noexcept
{
    if (hstmt != SQL_NULL_HSTMT)
    {
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        hstmt = SQL_NULL_HSTMT;
    }
    if (hdbc != SQL_NULL_HDBC)
    {
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        hdbc = SQL_NULL_HDBC;
    }
    if (henv != SQL_NULL_HENV)
    {
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        henv = SQL_NULL_HENV;
    }
}

void ODBCSink::start(PipelineExecutionContext&)
{
    NES_INFO("Opening ODBCSink on table `{}` of database `{}` at {}:{}.", table, database, host, port);
    /// A throw partway through leaves some handles allocated; ~ODBCSink runs
    /// closeConnection() once the engine destroys the failed sink, so there is
    /// no need to unwind the half-open connection here.
    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv) != SQL_SUCCESS)
    {
        throw CannotOpenSink("ODBCSink: failed to allocate ODBC environment handle");
    }
    if (const auto diag
        = odbcDiagnostics(SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0), SQL_HANDLE_ENV, henv))
    {
        throw CannotOpenSink("ODBCSink: failed to set ODBC version:{}", *diag);
    }
    if (const auto diag = odbcDiagnostics(SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc), SQL_HANDLE_ENV, henv))
    {
        throw CannotOpenSink("ODBCSink: failed to allocate connection handle:{}", *diag);
    }

    /// Same connection-string layout as ODBCSource::open, so the sink
    /// reaches a database addressed exactly the way the source does.
    auto connectionString
        = fmt::format("DRIVER={{{}}};SERVER={};PORT={};DATABASE={};UID={};PWD={};", driver, host, port, database, username, password);
    if (const auto diag = odbcDiagnostics(
            SQLDriverConnect(
                hdbc, nullptr, reinterpret_cast<SQLCHAR*>(connectionString.data()), SQL_NTS, nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT),
            SQL_HANDLE_DBC,
            hdbc))
    {
        throw CannotOpenSink("ODBCSink: failed to connect to {}:{}:{}", host, port, *diag);
    }
    if (const auto diag = odbcDiagnostics(SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt), SQL_HANDLE_DBC, hdbc))
    {
        throw CannotOpenSink("ODBCSink: failed to allocate statement handle:{}", *diag);
    }
    NES_DEBUG("ODBCSink connected successfully.");
}

void ODBCSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(hstmt != SQL_NULL_HSTMT, "ODBCSink is not started");
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in ODBCSink.");

    /// The buffer reaching a sink holds the formatted output; for a formatted
    /// buffer `numberOfTuples` is the byte length, and overflow spills into
    /// child buffers. Concatenate main + children into one blob, exactly as
    /// MQTTSink assembles its publish payload.
    std::string formatted;
    const auto main = inputTupleBuffer.getAvailableMemoryArea<char>().first(inputTupleBuffer.getNumberOfTuples());
    formatted.append(main.begin(), main.end());
    for (size_t index = 0; index < inputTupleBuffer.getNumberOfChildBuffers(); ++index)
    {
        const auto child = inputTupleBuffer.loadChildBuffer(VariableSizedAccess::Index(index));
        const auto childData = child.getAvailableMemoryArea<char>().first(child.getNumberOfTuples());
        formatted.append(childData.begin(), childData.end());
    }

    /// One INSERT per formatted row. Autocommit stays on, so every row is
    /// visible to a concurrent reader as soon as it lands — the polling sink
    /// capture in ODBCSinkCapture.cpp relies on that.
    std::size_t pos = 0;
    while (pos < formatted.size())
    {
        const auto newline = formatted.find('\n', pos);
        const auto rawRow = std::string_view(formatted).substr(pos, newline == std::string::npos ? std::string::npos : newline - pos);
        const auto row = stripLineEnding(rawRow);
        if (!row.empty())
        {
            auto statement = buildInsert(table, row);
            if (const auto diag
                = odbcDiagnostics(SQLExecDirect(hstmt, reinterpret_cast<SQLCHAR*>(statement.data()), SQL_NTS), SQL_HANDLE_STMT, hstmt))
            {
                throw RunningRoutineFailure("ODBCSink: INSERT into `{}` failed:{}", table, *diag);
            }
            ++insertedRows;
        }
        if (newline == std::string::npos)
        {
            break;
        }
        pos = newline + 1;
    }
}

void ODBCSink::stop(PipelineExecutionContext&)
{
    closeConnection();
    NES_INFO("ODBCSink completed: inserted {} row(s) into `{}`.", insertedRows, table);
}

DescriptorConfig::Config ODBCSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersODBCSink>(std::move(config), NAME);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): registry signature fixed by framework.
SinkValidationRegistryReturnType RegisterODBCSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return ODBCSink::validateAndFormat(std::move(sinkConfig.config));
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): registry signature fixed by framework.
SinkRegistryReturnType RegisterODBCSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<ODBCSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
