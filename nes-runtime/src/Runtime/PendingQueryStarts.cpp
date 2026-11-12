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

#include <Runtime/PendingQueryStarts.hpp>

#include <functional>
#include <QueryId.hpp>

namespace NES
{

void PendingQueryStarts::add(QueryId queryId)
{
    stopRequested.wlock()->insert_or_assign(queryId, false);
}

void PendingQueryStarts::remove(QueryId queryId)
{
    stopRequested.wlock()->erase(queryId);
}

bool PendingQueryStarts::stop(QueryId queryId)
{
    const auto locked = stopRequested.wlock();
    const auto pending = locked->find(queryId);
    if (pending == locked->end())
    {
        return false;
    }
    pending->second = true;
    return true;
}

bool PendingQueryStarts::finish(QueryId queryId, const std::function<void()>& handOver)
{
    const auto locked = stopRequested.wlock();
    const auto pending = locked->find(queryId);
    const bool stopped = pending != locked->end() && pending->second;
    if (pending != locked->end())
    {
        locked->erase(pending);
    }
    if (stopped)
    {
        return false;
    }
    handOver();
    return true;
}
}
