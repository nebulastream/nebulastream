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

#include <SystestRunnerUtil.hpp>

#include <ranges>

#include <Listeners/QueryLog.hpp>
#include <ErrorHandling.hpp>

namespace NES::Systest
{

std::vector<Exception> getExceptions(const QueryStatus& status)
{
    std::vector<Exception> exceptions;
    for (const auto& localStatus : status.localStatusSnapshots)
    {
        if (auto err = localStatus.metrics.error)
        {
            exceptions.push_back(*err);
        }
    }
    return exceptions;
}

QueryState getGlobalQueryState(const QueryStatus& status)
{
    /// Query if considered failed if any local query failed
    if (std::ranges::any_of(status.localStatusSnapshots, [](const LocalQueryStatus& local) { return local.state == QueryState::Failed; }))
    {
        return QueryState::Failed;
    }
    /// Query is not failed, stopped if all local queries have stopped
    if (std::ranges::all_of(status.localStatusSnapshots, [](const LocalQueryStatus& local) { return local.state == QueryState::Stopped; }))
    {
        return QueryState::Stopped;
    }
    /// Query is neither failed nor stopped. Running, if all local queries are running
    if (std::ranges::all_of(status.localStatusSnapshots, [](const LocalQueryStatus& local) { return local.state == QueryState::Running; }))
    {
        return QueryState::Running;
    }
    /// Query is neither failed nor stopped, nor running. Started, if all local queries have started
    if (std::ranges::all_of(status.localStatusSnapshots, [](const LocalQueryStatus& local) { return local.state == QueryState::Started; }))
    {
        return QueryState::Started;
    }
    /// Some local queries might be stopped, running, or started, but at least one local query has not been started
    return QueryState::Registered;
}

}