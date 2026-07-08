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

#include <QueryManager/QueryManagementUtils.hpp>

namespace NES::QueryManagementUtils
{
std::jthread stopLocalQuery(LocalQuery& query, QuerySubmissionBackend& backend)
{
    /// this routine will run until the target query is either stopped,
    /// OR the worker crashes and restarts (without the query)
    /// => afterwards the query is definetly not running anymore
    return std::jthread(
        [&]
        {
            while (true)
            {
                query.waitForStop(); /// enter waiting management state, which allows heartbeat thread to release it
                if (!query.checkMaybeRunning())
                {
                    break;
                }
                // TODO use terminate for end of query stops?
                auto stopResponse = backend.terminate(query.getCurrentQueryId());
                if (stopResponse)
                {
                    /// stop was initiated by worker
                    // at this point the worker will either stop successfully, or crash and restart without the query
                    while (query.checkMaybeRunning())
                    {
                        // let heartbeat thread do its work
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    }
                }
                else
                {
                    /// no response, try again later
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }
        });
}

std::jthread restartLocalQuery(LocalQuery& query, QuerySubmissionBackend& backend)
{
    return std::jthread(
        [&]
        {
            /// start new epoch
            auto newEpochId = query.getCurrentQueryId().nextEpoch();
            auto& plan = query.getLocalPlan();
            plan.setQueryId(newEpochId);

            /// attempt to restart query until the worker responds
            while (true)
            {
                auto newId = backend.start(plan);
                if (newId || newId.error().code() == 2035) // TODO query already registered, inject correct status code
                {
                    query.setNewQueryId(newId.value());
                    query.reset();
                    break;
                }
                else
                {
                    /// wait and retry
                    /// the query may be running at this point (if the response was lost)
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }
        });
}

void checkLocalQueryStatus(LocalQuery& query, QuerySubmissionBackend& backend, bool injectFailure)
{
    auto queryId = query.getCurrentQueryId();
    auto status = backend.status(queryId);

    if (status && !injectFailure)
    {
        query.heartbeat(true);
        auto& snapshot = status.value();
        query.updateSnapshot(snapshot, queryId);
    }
    else
    {
        auto exception = !status ? status.error() : Exception{"injected failure", 5000};
        if (exception.code() == 5000) // TODO = QueryNotFound; inject the error code more cleanly
        {
            query.heartbeat(true);
            query.markMissing(queryId);
        }
        else if (exception.code() == 5005) // query status failed / worker unavailable
        {
            query.heartbeat(false);
        }
        /// TODO what about other error codes
    }
}
}
