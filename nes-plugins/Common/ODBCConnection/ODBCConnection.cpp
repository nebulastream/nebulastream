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

#include <ODBCConnection.hpp>

#include <array>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <sstream>
#include <string>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>

/// The ODBC C API speaks in `SQLCHAR*` (an unsigned-char alias) and takes handles through
/// `void*`/`SQLPOINTER`, so bridging NebulaStream's buffers to it unavoidably requires
/// reinterpret_cast across that C-API boundary.
///
/// `rc` (SQLRETURN) is the unixODBC convention and appears at every API call; the
/// readability-identifier-length band keeps grep-ability for ODBC-versed readers.
/// ODBC also passes integer attribute values (ODBC version, timeouts) through a SQLPOINTER, so
/// the int-to-pointer casts are mandated by the API rather than an optimization hazard.
/// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast,readability-identifier-length,performance-no-int-to-ptr)
namespace NES::ODBC
{

namespace
{

/// SQLSTATE is a fixed 5-character code; the buffer holds 5 chars + NUL terminator.
constexpr SQLSMALLINT SQL_STATE_BUFFER_SIZE = 6;
/// Buffer size for the connection string the driver echoes back from SQLDriverConnect.
constexpr SQLSMALLINT OUT_CONNECTION_STRING_SIZE = 1024;

std::string formatConnectionString(const ConnectionInformation& info, const std::string& password)
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
            password,
            info.trustServerCertificate ? "yes" : "no");
    }
    return fmt::format(
        "DRIVER={{{}}};SERVER={};PORT={};DATABASE={};UID={};PWD={};CharSet=UTF-8",
        info.driver,
        info.server,
        info.port,
        info.database,
        info.username,
        password);
}

/// vcpkg's statically-linked unixODBC does not default its ODBCSYSINI to the system /etc, so
/// drivers registered there (e.g. msodbcsql18's "ODBC Driver 18 for SQL Server" in
/// /etc/odbcinst.ini) are invisible and SQLDriverConnect fails with "Can't open lib ... : file not
/// found". Point it at /etc so the driver manager resolves the DRIVER={..} name.
///
/// setenv mutates the process-global environ block and is not thread safe. A source opening its
/// connection concurrently with a LOOKBACK call would otherwise race here, so this runs once.
void ensureDriverManagerPathsOnce()
{
    static std::once_flag onceFlag;
    std::call_once(
        onceFlag,
        []
        {
            /// POSIX-only, hence no std:: header declares them; call_once is what makes the
            /// environ mutation safe here.
            /// NOLINTBEGIN(concurrency-mt-unsafe,misc-include-cleaner)
            setenv("ODBCSYSINI", "/etc", 1);
            setenv("ODBCINI", "/etc/odbc.ini", 1);
            /// NOLINTEND(concurrency-mt-unsafe,misc-include-cleaner)
        });
}

}

std::string buildConnectionString(const ConnectionInformation& info)
{
    return formatConnectionString(info, info.password);
}

std::string buildRedactedConnectionString(const ConnectionInformation& info)
{
    return formatConnectionString(info, "***");
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

Connection::Connection(const ConnectionInformation& connectionInformation, SQLUINTEGER loginTimeoutSeconds)
{
    ensureDriverManagerPathsOnce();

    auto connectionString = buildConnectionString(connectionInformation);

    /// Never log the real connection string — it carries PWD=.
    NES_DEBUG("Connecting to database: {}", buildRedactedConnectionString(connectionInformation));
    {
        SQLHANDLE tmp = SQL_NULL_HENV;
        const auto rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &tmp);
        NES_ODBC_CHECK(rc, SQL_HANDLE_ENV, tmp, "SQLAllocHandle ENV");
        this->env = EnvironmentHandle{tmp};
    }

    SQLSetEnvAttr(env.get(), SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0);

    {
        SQLHANDLE tmp = SQL_NULL_HDBC;
        const auto rc = SQLAllocHandle(SQL_HANDLE_DBC, env.get(), &tmp);
        NES_ODBC_CHECK(rc, SQL_HANDLE_DBC, tmp, "SQLAllocHandle DBC");
        this->connection = ConnectionHandle{tmp};
    }

    if (loginTimeoutSeconds != 0)
    {
        /// Best effort: a driver that does not support the attribute must not stop us connecting.
        SQLSetConnectAttr(
            connection.get(), SQL_ATTR_LOGIN_TIMEOUT, reinterpret_cast<SQLPOINTER>(static_cast<std::uintptr_t>(loginTimeoutSeconds)), 0);
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
    NES_ODBC_CHECK(rc, SQL_HANDLE_DBC, connection.get(), "SQLDriverConnect");
}

}

/// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast,readability-identifier-length,performance-no-int-to-ptr)
