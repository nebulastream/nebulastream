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

#include <PlacementAmendmentMode.hpp>

#include <algorithm>
#include <chrono>
#include <ranges>
#include <set>
#include <thread>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{

LockedWorkers::LockedWorkers(WorkerCatalog& workerCatalog, std::vector<Host> workers)
    : workerCatalog(&workerCatalog), workers(std::move(workers))
{
}

LockedWorkers::LockedWorkers(LockedWorkers&& other) noexcept : workerCatalog(other.workerCatalog), workers(std::exchange(other.workers, {}))
{
}

LockedWorkers::~LockedWorkers()
{
    /// Release the locks in the inverse order of their acquisition.
    for (const auto& worker : workers | std::views::reverse)
    {
        workerCatalog->unlockWorker(worker);
    }
}

LockedWorkers LockedWorkers::lock(WorkerCatalog& workerCatalog, const std::set<Host>& workers)
{
    constexpr auto INITIAL_BACKOFF = std::chrono::microseconds(50);
    constexpr auto MAX_BACKOFF = std::chrono::milliseconds(50);
    for (auto backoff = std::chrono::duration_cast<std::chrono::microseconds>(INITIAL_BACKOFF);;
         backoff = std::min(backoff * 2, std::chrono::duration_cast<std::chrono::microseconds>(MAX_BACKOFF)))
    {
        std::vector<Host> locked;
        for (const auto& worker : workers)
        {
            if (not workerCatalog.tryLockWorker(worker))
            {
                break;
            }
            locked.emplace_back(worker);
        }
        if (locked.size() == workers.size())
        {
            return {workerCatalog, std::move(locked)};
        }
        for (const auto& worker : locked | std::views::reverse)
        {
            workerCatalog.unlockWorker(worker);
        }
        std::this_thread::sleep_for(backoff);
    }
}

}
