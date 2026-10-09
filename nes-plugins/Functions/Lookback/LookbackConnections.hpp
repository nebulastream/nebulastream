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

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <ODBCConnection.hpp>

namespace NES::Lookback
{

/// Bounds on one lookback connection. They exist because a LOOKBACK call runs on a worker thread
/// inside a query pipeline: an unbounded result or an unbounded wait would stall ingestion.
struct LookbackLimits
{
    /// Two seconds: long enough for an indexed point lookup over a hospital LAN, short enough that
    /// a stalled database delays one alert rather than the ingest pipeline behind it.
    static constexpr uint32_t DEFAULT_QUERY_TIMEOUT_MS = 2000;
    /// A lookback is context for a human-readable alert, not a result set.
    static constexpr uint32_t DEFAULT_MAX_ROWS = 10;
    /// Comfortably above any alert message a clinician would read, well below a buffer's worth.
    static constexpr uint32_t DEFAULT_MAX_RESULT_BYTES = 8192;

    /// Login and query timeout handed to the driver. 0 leaves the driver default in place.
    uint32_t queryTimeoutMs = DEFAULT_QUERY_TIMEOUT_MS;
    /// Rows rendered into the result string; further rows are dropped and logged.
    uint32_t maxRows = DEFAULT_MAX_ROWS;
    /// Hard cap on the rendered string; a longer rendering is truncated and logged.
    uint32_t maxResultBytes = DEFAULT_MAX_RESULT_BYTES;
    /// false (the default): a database failure yields an empty lookback string and the alert is
    /// still delivered. true: the failure propagates and fails the pipeline. An alert that fires
    /// is clinically more important than the context attached to it, hence the default.
    bool strict = false;
};

/// One named lookback target. Exactly one of `odbc` / `staticRows` is set.
struct LookbackConnection
{
    std::string name;
    LookbackLimits limits;
    /// A real database.
    std::optional<ODBC::ConnectionInformation> odbc;
    /// A test fixture: these rows are returned for every call, no database involved. This is what
    /// lets the systests exercise the whole parser → optimizer → pipeline path in CI.
    std::optional<std::vector<std::vector<std::string>>> staticRows;
};

/// Process-wide table of named lookback connections, loaded from the YAML file named by
/// `NES_LOOKBACK_CONFIG` the first time a connection is looked up.
///
/// A function has no handle on worker configuration — `PhysicalFunctionRegistryArguments` carries
/// only child functions and types — so the connection cannot be plumbed in through the registry.
/// Naming the connection in the query and resolving it here is what keeps database credentials out
/// of query text, which matters because these queries are pasted around in tickets and logs.
class LookbackConnectionRegistry
{
public:
    static LookbackConnectionRegistry& instance();

    /// @throws UnknownLookbackConnection if `name` is not configured.
    [[nodiscard]] LookbackConnection get(const std::string& name);

    [[nodiscard]] bool contains(const std::string& name);

    /// Replaces the table with the connections parsed from `yamlText`. Intended for tests; the
    /// production path is the lazy load from `NES_LOOKBACK_CONFIG`.
    void loadFromYaml(const std::string& yamlText);

    /// Parses the YAML document without touching the registry. Exposed for tests and for the
    /// lazy loader.
    static std::unordered_map<std::string, LookbackConnection> parse(const std::string& yamlText);

    /// Expands `${VAR}` occurrences from the environment. An unset variable is an error rather
    /// than an empty expansion, so a missing password fails loudly instead of attempting an
    /// anonymous connect.
    static std::string expandEnvironment(const std::string& value);

private:
    void ensureLoaded();

    std::mutex mutex;
    bool loaded = false;
    std::unordered_map<std::string, LookbackConnection> connections;
};

}
