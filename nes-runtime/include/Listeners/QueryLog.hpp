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
#include <optional>
#include <ostream>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/AbstractQueryStatusListener.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>

namespace NES
{

struct QueryRunSummary
{
    std::optional<std::chrono::system_clock::time_point> start;
    std::optional<std::chrono::system_clock::time_point> running;
    std::optional<std::chrono::system_clock::time_point> stop;
    std::optional<Exception> error;
};

/// Summary structure of the query log for a query
struct QuerySummary
{
    /// In the future, this will be extended to include more information such as the query plan, etc.
    QueryId queryId = INVALID_QUERY_ID;
    QueryStatus currentStatus;
    std::vector<QueryRunSummary> runs;
};

/// Struct to store the status change of a query. Initialized either with a status or an exception.
struct QueryStatusChange
{
    QueryStatusChange(QueryStatus state, std::chrono::system_clock::time_point timestamp) : state(state), timestamp(timestamp) { };

    QueryStatusChange(Exception exception, std::chrono::system_clock::time_point timestamp)
        : state(QueryStatus::Failed), timestamp(timestamp), exception(exception) { };

    friend std::ostream& operator<<(std::ostream& os, const QueryStatusChange& statusChange);

    QueryStatus state;
    std::chrono::system_clock::time_point timestamp;
    std::optional<Exception> exception;
};
inline std::ostream& operator<<(std::ostream& os, const QueryStatusChange& statusChange);

/// The query log keeps track of query status changes. We want to keep it as lightweight as possible to reduce overhead inflicted to
/// the query manager.
struct QueryLog : AbstractQueryStatusListener
{
    using Log = std::vector<QueryStatusChange>;
    using QueryStatusLog = std::unordered_map<QueryId, std::vector<QueryStatusChange>>;

    /// TODO #241: we should use the new unique sourceId/hash once implemented here instead
    bool logSourceTermination(
        QueryId queryId, OriginId sourceId, QueryTerminationType, std::chrono::system_clock::time_point timestamp) override;
    bool logQueryFailure(QueryId queryId, Exception exception, std::chrono::system_clock::time_point timestamp) override;
    bool logQueryStatusChange(QueryId queryId, QueryStatus status, std::chrono::system_clock::time_point timestamp) override;

    [[nodiscard]] std::optional<Log> getLogForQuery(QueryId queryId);
    [[nodiscard]] std::optional<QuerySummary> getQuerySummary(QueryId queryId);
    [[nodiscard]] std::vector<QuerySummary> getSummary();

private:
    folly::Synchronized<QueryStatusLog> queryStatusLog;
};
}
