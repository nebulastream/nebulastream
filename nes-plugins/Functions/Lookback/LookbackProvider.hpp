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
#include <optional>
#include <string>
#include <vector>
#include <sqltypes.h>
#include <LookbackConnections.hpp>
#include <ODBCConnection.hpp>

namespace NES::Lookback
{

/// One lookback result: rows of stringified column values. Every column is pulled as text —
/// the rendering target is a string, so converting through a typed representation would only add
/// a lossy round trip.
using ResultRows = std::vector<std::vector<std::string>>;

/// Executes the lookback query. The seam exists so the whole function can be exercised in CI
/// against canned rows, with no database in the loop.
class LookbackProvider
{
public:
    LookbackProvider() = default;
    LookbackProvider(const LookbackProvider&) = delete;
    LookbackProvider& operator=(const LookbackProvider&) = delete;
    LookbackProvider(LookbackProvider&&) = delete;
    LookbackProvider& operator=(LookbackProvider&&) = delete;
    virtual ~LookbackProvider() = default;

    /// @param parameters values for the SQL's `?` markers, in order.
    virtual ResultRows run(const std::vector<std::string>& parameters) = 0;
};

/// Returns the connection's canned rows for every call, ignoring the parameters.
class StaticLookbackProvider final : public LookbackProvider
{
public:
    explicit StaticLookbackProvider(ResultRows rows);
    ResultRows run(const std::vector<std::string>& parameters) override;

private:
    ResultRows rows;
};

/// Connects lazily on the first call and keeps the connection and the prepared statement for the
/// lifetime of the instance — the SQL is fixed at query-compile time, so preparing once per thread
/// is free. One instance is owned per worker thread; ODBC handles are not thread safe.
class OdbcLookbackProvider final : public LookbackProvider
{
public:
    OdbcLookbackProvider(ODBC::ConnectionInformation connectionInformation, std::string sql, LookbackLimits limits);
    ResultRows run(const std::vector<std::string>& parameters) override;

private:
    void ensurePrepared();

    ODBC::ConnectionInformation connectionInformation;
    std::string sql;
    LookbackLimits limits;

    std::optional<ODBC::Connection> connection;
    ODBC::StmtHandle statement;
    /// Parameter descriptions from SQLDescribeParam, or the SQL_VARCHAR fallback for drivers that
    /// cannot describe. Sized on first prepare.
    std::vector<SQLSMALLINT> parameterSqlTypes;
    /// Staging storage the driver reads from during SQLExecute. Held as members so the addresses
    /// handed to SQLBindParameter stay valid across the call.
    std::vector<std::string> boundParameters;
    std::vector<SQLLEN> boundIndicators;
};

/// Builds the provider a connection calls for: the fixture provider for a `rows:` connection, the
/// ODBC provider otherwise.
std::unique_ptr<LookbackProvider> makeProvider(const LookbackConnection& connection, const std::string& sql);

}
