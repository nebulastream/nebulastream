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

#include <memory>
#include <string>
#include <sql.h>
#include <sqltypes.h>
/// Both back NES_ODBC_CHECK below, which expands NES_WARNING and a NES exception at the call
/// site; nothing in this header names them directly.
#include <Util/Logger/Logger.hpp> /// NOLINT(misc-include-cleaner)
#include <ErrorHandling.hpp> /// NOLINT(misc-include-cleaner)

/// Shared ODBC connection core. Extracted from the ODBC source so that every component talking
/// ODBC — the source, the LOOKBACK function, and eventually the sink — opens connections the same
/// way instead of each carrying its own copy of the driver-dialect and diagnostics logic.
namespace NES::ODBC
{

/// Everything needed to reach one database. Deliberately a plain aggregate: each consumer fills it
/// from its own configuration surface (source descriptor keys, lookback connection file, ...).
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
std::string buildConnectionString(const ConnectionInformation& info);

/// Same as buildConnectionString but with the password replaced by `***`. Use this for anything
/// that reaches a log — the real connection string carries clinical-database credentials.
std::string buildRedactedConnectionString(const ConnectionInformation& info);

/// Concatenates every diagnostic record the driver has queued for `handle` into one message.
std::string showError(SQLSMALLINT handleType, SQLHANDLE handle, const std::string& context);

/// A constexpr template can't replicate this: the throw needs the caller's source location for the
/// exception's stack trace; the macro form preserves __FILE__:__LINE__. `NES_ODBC_CHECK_AS` lets a
/// caller pick the exception type so a lookback failure does not surface as "failed to open a
/// source".
/// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define NES_ODBC_CHECK_AS(rc, handleType, handle, context, ExceptionType) \
    do \
    { \
        const SQLRETURN check_rc = (rc); \
        if (check_rc != SQL_SUCCESS && check_rc != SQL_SUCCESS_WITH_INFO) \
        { \
            if (check_rc == SQL_ERROR) \
            { \
                throw ExceptionType("{}: {}", (context), NES::ODBC::showError((handleType), (handle), (context))); \
            } \
            NES_WARNING("{}:{}", (context), NES::ODBC::showError((handleType), (handle), (context))); \
        } \
    } while (0)

/// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define NES_ODBC_CHECK(rc, handleType, handle, context) NES_ODBC_CHECK_AS(rc, handleType, handle, context, CannotOpenSource)

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

/// An open ODBC connection. Member order matters: `env` is declared first so it is destroyed last,
/// after the connection handle it parents.
///
/// Not thread safe. ODBC handles must be touched by one thread at a time; every consumer either
/// confines a Connection to one thread or guards it with its own lock.
struct Connection
{
    EnvironmentHandle env;
    ConnectionHandle connection;

    /// @param loginTimeoutSeconds 0 leaves the driver default in place.
    Connection(const ConnectionInformation& connectionInformation, SQLUINTEGER loginTimeoutSeconds);
};

}
