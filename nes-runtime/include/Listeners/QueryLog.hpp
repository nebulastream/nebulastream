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

#ifndef NES_RUNTIME_INCLUDE_LISTENERS_QUERYLOG_HPP_
#define NES_RUNTIME_INCLUDE_LISTENERS_QUERYLOG_HPP_

#include <shared_mutex>
#include <Exceptions/Exception.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/AbstractQueryStatusListener.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <magic_enum.hpp>

namespace NES::Runtime
{

/// Struct to store the status change of a query. Initialized either with a status or an exception.
struct QueryStatusChange
{
    QueryStatusChange(Execution::QueryStatus state) : state(state), timestamp(std::chrono::system_clock::now()){};

    QueryStatusChange(const Exception& exception)
        : state(Execution::QueryStatus::Failed), timestamp(std::chrono::system_clock::now()), exception(exception){};

    [[nodiscard]] std::string toString() const;
    friend std::ostream& operator<<(std::ostream& os, const QueryStatusChange& statusChange);

    Execution::QueryStatus state;
    std::chrono::system_clock::time_point timestamp;
    std::optional<std::reference_wrapper<const Exception>> exception;
};

inline std::ostream& operator<<(std::ostream& os, const QueryStatusChange& statusChange)
{
    os << statusChange.toString();
    return os;
};


/// In the future, we might want to use a threadsafe map here instead (e.g. folly/AtomicHashmap.h)
using QueryStatusLog = std::unordered_map<QueryId, std::vector<QueryStatusChange>>;

/// The query log keeps track of query status changes. We want to keep it as lightweight as possible to reduce overhead inflicted to
/// the query manager.
struct QueryLog : public AbstractQueryStatusListener
{
    bool canTriggerEndOfStream(QueryId queryId, OperatorId sourceId, Runtime::QueryTerminationType) override;
    bool notifySourceTermination(QueryId queryId, OperatorId sourceId, Runtime::QueryTerminationType) override;
    bool notifyQueryFailure(QueryId queryId, const Exception& exception) override;
    bool notifyQueryStatusChange(QueryId queryId, Runtime::Execution::QueryStatus Status) override;
    bool notifyEpochTermination(QueryId queryId, uint64_t timestamp) override;

    /// Get latest query status change for a query. Invalid status if query with id does not exist.
    [[nodiscard]] QueryStatusChange getQueryStatus(QueryId queryId);
    /// All exceptions that have occurred during the query lifetime.
    [[nodiscard]] std::vector<Exception> getExceptions(QueryId queryId);
    /// How many times was a query restarted: stopped --> running, failed --> running
    [[nodiscard]] uint64_t getNumberOfRestarts(QueryId queryId);
    /// Status log of the query in the format: {[STATUS TIMESTAMP], ...}
    [[nodiscard]] std::vector<std::string> getStatusLog(QueryId queryId);

private:
    QueryStatusLog queryStatusLog;
    /// We use a shared mutex to enable concurrent reads
    std::shared_mutex statusMutex;
};
using QueryLogPtr = std::shared_ptr<QueryLog>;
} /// namespace NES
#endif /// NES_RUNTIME_INCLUDE_LISTENERS_QUERYLOG_HPP_
