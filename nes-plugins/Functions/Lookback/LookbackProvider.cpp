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

#include <LookbackProvider.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <LookbackConnections.hpp>
#include <ODBCConnection.hpp>

/// See ODBCConnection.cpp: the ODBC C API takes buffers and handles through void*/SQLPOINTER, so
/// every cast here is mandated by the unixODBC headers. `rc` is the unixODBC convention.
/// The query-timeout attribute is likewise an integer passed through a SQLPOINTER.
/// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast,readability-identifier-length,performance-no-int-to-ptr)
namespace NES::Lookback
{

namespace
{

/// Milliseconds are the configuration unit because every other NES timeout is; ODBC takes whole
/// seconds. Round up so a sub-second configuration never degrades into "no timeout".
SQLUINTEGER toOdbcSeconds(uint32_t milliseconds)
{
    if (milliseconds == 0)
    {
        return 0;
    }
    constexpr uint32_t millisecondsPerSecond = 1000;
    return (milliseconds + millisecondsPerSecond - 1) / millisecondsPerSecond;
}

}

StaticLookbackProvider::StaticLookbackProvider(ResultRows rowsIn) : rows(std::move(rowsIn))
{
}

ResultRows StaticLookbackProvider::run(const std::vector<std::string>& parameters)
{
    /// The fixture is parameter-independent on purpose: a systest asserting on a fixed alert string
    /// should not have to model the database's filtering too.
    NES_TRACE("Static lookback provider returning {} row(s) for {} parameter(s)", rows.size(), parameters.size());
    return rows;
}

OdbcLookbackProvider::OdbcLookbackProvider(ODBC::ConnectionInformation connectionInformationIn, std::string sqlIn, LookbackLimits limitsIn)
    : connectionInformation(std::move(connectionInformationIn)), sql(std::move(sqlIn)), limits(limitsIn)
{
}

void OdbcLookbackProvider::ensurePrepared()
{
    if (statement)
    {
        return;
    }

    const auto timeoutSeconds = toOdbcSeconds(limits.queryTimeoutMs);
    if (!connection)
    {
        connection.emplace(connectionInformation, timeoutSeconds);
    }

    SQLHANDLE handle = SQL_NULL_HSTMT;
    auto rc = SQLAllocHandle(SQL_HANDLE_STMT, connection->connection.get(), &handle);
    NES_ODBC_CHECK_AS(rc, SQL_HANDLE_DBC, connection->connection.get(), "SQLAllocHandle STMT", LookbackFailure);
    auto owned = ODBC::StmtHandle{handle};

    if (timeoutSeconds != 0)
    {
        /// Best effort: a driver that cannot honour the attribute must not stop the lookback.
        SQLSetStmtAttr(handle, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(static_cast<std::uintptr_t>(timeoutSeconds)), 0);
    }

    rc = SQLPrepare(handle, reinterpret_cast<SQLCHAR*>(sql.data()), SQL_NTS);
    NES_ODBC_CHECK_AS(rc, SQL_HANDLE_STMT, handle, "SQLPrepare", LookbackFailure);

    SQLSMALLINT parameterCount = 0;
    rc = SQLNumParams(handle, &parameterCount);
    NES_ODBC_CHECK_AS(rc, SQL_HANDLE_STMT, handle, "SQLNumParams", LookbackFailure);

    parameterSqlTypes.assign(static_cast<size_t>(parameterCount), SQL_VARCHAR);
    for (SQLSMALLINT parameter = 1; parameter <= parameterCount; ++parameter)
    {
        SQLSMALLINT dataType = 0;
        SQLULEN parameterSize = 0;
        SQLSMALLINT decimalDigits = 0;
        SQLSMALLINT nullable = 0;
        /// Not every driver implements SQLDescribeParam. Binding as SQL_VARCHAR still works — the
        /// server coerces — it just costs the typed comparison a conversion, so describe when we
        /// can and fall back quietly when we cannot.
        if (SQLDescribeParam(handle, parameter, &dataType, &parameterSize, &decimalDigits, &nullable) == SQL_SUCCESS)
        {
            parameterSqlTypes[parameter - 1] = dataType;
        }
        else
        {
            NES_DEBUG("SQLDescribeParam failed for parameter {}; binding it as SQL_VARCHAR", parameter);
        }
    }

    statement = std::move(owned);
}

ResultRows OdbcLookbackProvider::run(const std::vector<std::string>& parameters)
{
    ensurePrepared();
    auto* const handle = statement.get();

    if (parameters.size() != parameterSqlTypes.size())
    {
        throw LookbackFailure("Lookback query expects {} parameter(s) but {} were supplied", parameterSqlTypes.size(), parameters.size());
    }

    /// Assign into the members before binding: the driver keeps the pointers until SQLExecute
    /// returns, so they must not be dangling copies of a temporary.
    boundParameters = parameters;
    boundIndicators.assign(parameters.size(), 0);
    for (size_t i = 0; i < boundParameters.size(); ++i)
    {
        auto& value = boundParameters[i];
        boundIndicators[i] = static_cast<SQLLEN>(value.size());
        /// A zero column size is rejected by some drivers; a parameter is at least one character
        /// wide as far as the driver is concerned, even when the value is empty.
        const auto columnSize = std::max<SQLULEN>(value.size(), 1);
        const auto rc = SQLBindParameter(
            handle,
            static_cast<SQLUSMALLINT>(i + 1),
            SQL_PARAM_INPUT,
            SQL_C_CHAR,
            parameterSqlTypes[i],
            columnSize,
            0,
            value.data(),
            static_cast<SQLLEN>(value.size()),
            &boundIndicators[i]);
        NES_ODBC_CHECK_AS(rc, SQL_HANDLE_STMT, handle, "SQLBindParameter", LookbackFailure);
    }

    auto rc = SQLExecute(handle);
    NES_ODBC_CHECK_AS(rc, SQL_HANDLE_STMT, handle, "SQLExecute", LookbackFailure);

    SQLSMALLINT columnCount = 0;
    rc = SQLNumResultCols(handle, &columnCount);
    NES_ODBC_CHECK_AS(rc, SQL_HANDLE_STMT, handle, "SQLNumResultCols", LookbackFailure);

    ResultRows rows;
    bool truncated = false;
    while (true)
    {
        rc = SQLFetch(handle);
        if (rc == SQL_NO_DATA)
        {
            break;
        }
        NES_ODBC_CHECK_AS(rc, SQL_HANDLE_STMT, handle, "SQLFetch", LookbackFailure);

        if (rows.size() >= limits.maxRows)
        {
            truncated = true;
            break;
        }

        std::vector<std::string> row;
        row.reserve(static_cast<size_t>(columnCount));
        for (SQLSMALLINT column = 1; column <= columnCount; ++column)
        {
            /// Two-call idiom, as in the ODBC source: a one-byte buffer holds only the NUL
            /// terminator, so the driver consumes no data and reports the full length.
            std::array<SQLCHAR, 1> probe{};
            SQLLEN totalLength = 0;
            rc = SQLGetData(
                handle, static_cast<SQLUSMALLINT>(column), SQL_C_CHAR, probe.data(), static_cast<SQLLEN>(probe.size()), &totalLength);
            NES_ODBC_CHECK_AS(rc, SQL_HANDLE_STMT, handle, "SQLGetData", LookbackFailure);

            if (totalLength == SQL_NULL_DATA || totalLength == 0)
            {
                /// A SQL NULL renders as the empty string. The lookback's whole output is prose
                /// destined for an alert message; a missing value is best shown as nothing.
                row.emplace_back();
                continue;
            }
            if (totalLength == SQL_NO_TOTAL)
            {
                /// SQL Server cannot always report the length up front. Fall back to the result
                /// cap — the rendering is bounded by maxResultBytes anyway.
                totalLength = static_cast<SQLLEN>(limits.maxResultBytes);
            }

            std::string value(static_cast<size_t>(totalLength) + 1, '\0');
            SQLLEN bytesRead = 0;
            rc = SQLGetData(
                handle, static_cast<SQLUSMALLINT>(column), SQL_C_CHAR, value.data(), static_cast<SQLLEN>(value.size()), &bytesRead);
            NES_ODBC_CHECK_AS(rc, SQL_HANDLE_STMT, handle, "SQLGetData", LookbackFailure);
            value.resize(bytesRead == SQL_NO_TOTAL ? static_cast<size_t>(totalLength) : static_cast<size_t>(bytesRead));
            row.emplace_back(std::move(value));
        }
        rows.emplace_back(std::move(row));
    }

    /// Close the cursor but keep the prepared statement — the next alert reuses it.
    SQLFreeStmt(handle, SQL_CLOSE);

    if (truncated)
    {
        NES_WARNING("Lookback query returned more than max_rows={} rows; the surplus was dropped", limits.maxRows);
    }
    return rows;
}

std::unique_ptr<LookbackProvider> makeProvider(const LookbackConnection& connection, const std::string& sql)
{
    if (connection.staticRows.has_value())
    {
        return std::make_unique<StaticLookbackProvider>(connection.staticRows.value());
    }
    INVARIANT(connection.odbc.has_value(), "A lookback connection is either a fixture or a database; '{}' is neither", connection.name);
    return std::make_unique<OdbcLookbackProvider>(connection.odbc.value(), sql, connection.limits);
}

}

/// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast,readability-identifier-length,performance-no-int-to-ptr)
