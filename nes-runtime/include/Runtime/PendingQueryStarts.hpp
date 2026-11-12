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

#include <functional>
#include <unordered_map>
#include <folly/Synchronized.h>
#include <QueryId.hpp>

namespace NES
{

/// The queries that a worker has accepted and whose plan the engine has not received yet.
/// The engine ignores a stop for a query that it does not know, and the query would start afterwards.
/// A stop for such a query is recorded here, and the query is then not handed to the engine.
class PendingQueryStarts
{
public:
    void add(QueryId queryId);
    void remove(QueryId queryId);

    /// Returns false if the query is not pending. The stop then has to go to the engine.
    [[nodiscard]] bool stop(QueryId queryId);

    /// Removes the query and calls `handOver`, unless the query was stopped while it was pending.
    /// Returns whether `handOver` was called.
    /// The call is made under the lock, so the engine has received the query before a later stop is passed on to it.
    [[nodiscard]] bool finish(QueryId queryId, const std::function<void()>& handOver);

private:
    /// Maps each pending query to whether a stop arrived for it.
    folly::Synchronized<std::unordered_map<QueryId, bool>> stopRequested;
};
}
