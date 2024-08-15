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

#include <mutex>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <magic_enum.hpp>

namespace NES::Runtime
{

std::string QueryStatusChange::toString() const
{
    std::string result
        = std::string(magic_enum::enum_name(state)) + " : " + std::to_string(std::chrono::system_clock::to_time_t(timestamp));
    if (exception.has_value())
    {
        result += " with exception: " + exception.value().get().what();
    }
    return result;
}

std::vector<std::string> QueryLog::getStatusLog(NES::QueryId queryId)
{
    const std::shared_lock<std::shared_mutex> lock(statusMutex);
    auto log = queryStatusLog.at(queryId);
    std::vector<std::string> result;
    for (const auto& entry : log)
    {
        result.emplace_back(entry.toString());
    }
    return result;
}

bool QueryLog::canTriggerEndOfStream(QueryId, OperatorId, Runtime::QueryTerminationType)
{
    return true; /// nop
}

bool QueryLog::notifySourceTermination(QueryId, OperatorId, Runtime::QueryTerminationType)
{
    return true; /// nop
}

bool QueryLog::notifyQueryFailure(QueryId queryId, const Exception& exception)
{
    const std::unique_lock<std::shared_mutex> lock(statusMutex);
    if (queryStatusLog.contains(queryId))
    {
        queryStatusLog[queryId].emplace_back(exception);
        return true;
    }
    return false;
}

bool QueryLog::notifyQueryStatusChange(QueryId queryId, Runtime::Execution::QueryStatus status)
{
    const std::unique_lock<std::shared_mutex> lock(statusMutex);
    queryStatusLog[queryId].emplace_back(status);
    return true;
}

bool QueryLog::notifyEpochTermination(QueryId, uint64_t)
{
    return true; /// nop
}

QueryStatusChange QueryLog::getQueryStatus(QueryId queryId)
{
    std::shared_lock<std::shared_mutex> const lock(statusMutex);
    if (queryStatusLog.contains(queryId))
    {
        return queryStatusLog[queryId].back();
    }
    return {Execution::QueryStatus::Invalid};
}

std::vector<Exception> QueryLog::getExceptions(QueryId queryId)
{
    std::shared_lock<std::shared_mutex> const lock(statusMutex);
    std::vector<Exception> exceptions;
    if (queryStatusLog.contains(queryId))
    {
        for (const auto& statusChange : queryStatusLog.at(queryId))
        {
            if (statusChange.state == Execution::QueryStatus::Failed)
            {
                exceptions.push_back(statusChange.exception.value().get());
            }
        }
    }
    return exceptions;
}

uint64_t QueryLog::getNumberOfRestarts(QueryId queryId)
{
    std::shared_lock<std::shared_mutex> const lock(statusMutex);
    uint64_t restarts = 0;
    if (queryStatusLog.contains(queryId))
    {
        Execution::QueryStatus previousState = Execution::QueryStatus::Invalid;
        for (const auto& statusChange : queryStatusLog.at(queryId))
        {
            if ((previousState == Execution::QueryStatus::Failed || previousState == Execution::QueryStatus::Stopped)
                && statusChange.state == Execution::QueryStatus::Running)
            {
                ++restarts;
            }
            previousState = statusChange.state;
        }
    }
    return restarts;
}
} /// namespace NES::Runtime
