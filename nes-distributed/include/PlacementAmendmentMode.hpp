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
#include <set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{

/// How concurrent placement amendments of ISQP synchronize on the capacity of the workers (the original's PlacementAmendmentMode).
enum class PlacementAmendmentMode : uint8_t
{
    /// Optimistic concurrency control: place without locks, then occupy the slots atomically per worker; fail if they are gone.
    Optimistic,
    /// Strict two-phase locking: lock all workers of the selected path before placing, release the locks after occupying the slots.
    Pessimistic,
};

/// Holds the locks of a set of workers for a pessimistic placement amendment and releases them on destruction.
class LockedWorkers
{
public:
    /// Locks the workers in a fixed order. If a lock is held by another amendment, all locks acquired so far are released and the locking
    /// is retried with an exponential back-off, like the original's pessimistic path selection.
    static LockedWorkers lock(WorkerCatalog& workerCatalog, const std::set<Host>& workers);

    LockedWorkers(const LockedWorkers&) = delete;
    LockedWorkers& operator=(const LockedWorkers&) = delete;
    LockedWorkers(LockedWorkers&& other) noexcept;
    LockedWorkers& operator=(LockedWorkers&&) = delete;
    ~LockedWorkers();

private:
    LockedWorkers(WorkerCatalog& workerCatalog, std::vector<Host> workers);

    WorkerCatalog* workerCatalog;
    std::vector<Host> workers;
};

}
