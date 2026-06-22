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

#include <Runtime/Spill/SpillManager.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <vector>

#include <ErrorHandling.hpp>

namespace NES
{

SpillManager::SpillManager(SpillConfiguration configuration) : config(std::move(configuration))
{
    PRECONDITION(
        config.lowWatermark <= config.highWatermark,
        "SpillManager lowWatermark ({}) must not exceed highWatermark ({})",
        config.lowWatermark,
        config.highWatermark);
}

void SpillManager::registerState(const std::shared_ptr<SpillableState>& state)
{
    PRECONDITION(state != nullptr, "Cannot register a null SpillableState");
    const std::unique_lock lock(registryMutex);
    auto entry = std::make_shared<Entry>();
    entry->state = state;
    registry[state.get()] = std::move(entry);
}

void SpillManager::unregisterState(const SpillableState* state)
{
    const std::unique_lock lock(registryMutex);
    registry.erase(state);
}

std::shared_ptr<SpillManager::Entry> SpillManager::lookup(const SpillableState* state) const
{
    const std::shared_lock lock(registryMutex);
    if (const auto it = registry.find(state); it != registry.end())
    {
        return it->second;
    }
    return nullptr;
}

void SpillManager::pin(SpillableState& state)
{
    /// Unregistered states are not spillable (e.g. slices of operators that do not opt into spilling), so pinning them
    /// is a no-op. The generic build path may call pin() on any slice.
    const auto entry = lookup(&state);
    if (entry == nullptr)
    {
        return;
    }
    const std::lock_guard guard(entry->residencyMutex);
    /// Pin before any access: bump the count first (so a concurrent eviction sees it pinned), then reload if needed.
    entry->pinCount.fetch_add(1);
    if (state.isEvicted())
    {
        state.reloadState();
    }
}

void SpillManager::unpin(SpillableState& state)
{
    const auto entry = lookup(&state);
    if (entry == nullptr)
    {
        return;
    }
    USED_IN_DEBUG const auto previous = entry->pinCount.fetch_sub(1);
    INVARIANT(previous > 0, "unpin() called more times than pin() for a SpillableState");
}

size_t SpillManager::residentBytes() const
{
    const std::shared_lock lock(registryMutex);
    size_t total = 0;
    for (const auto& [ptr, entry] : registry)
    {
        if (const auto state = entry->state.lock())
        {
            total += state->residentBytes();
        }
    }
    return total;
}

size_t SpillManager::registeredCount() const
{
    const std::shared_lock lock(registryMutex);
    return registry.size();
}

size_t SpillManager::maybeSpill()
{
    if (!config.enabled || config.stateMemoryBudgetBytes == 0)
    {
        return 0;
    }
    const auto highBytes = static_cast<size_t>(static_cast<double>(config.stateMemoryBudgetBytes) * config.highWatermark);
    if (residentBytes() <= highBytes)
    {
        return 0;
    }
    const auto lowBytes = static_cast<size_t>(static_cast<double>(config.stateMemoryBudgetBytes) * config.lowWatermark);
    return evictDownTo(lowBytes);
}

size_t SpillManager::evictDownTo(size_t targetBytes)
{
    /// Snapshot live candidates (entry + a strong ref + its coldness) under a brief read-lock, then do the actual evict
    /// I/O outside it (under each unit's residency mutex) so disk I/O never serializes the whole registry. Holding the
    /// strong ref keeps each unit alive across the evict I/O; units being concurrently destroyed fail to lock and are skipped.
    struct Candidate
    {
        std::shared_ptr<Entry> entry;
        std::shared_ptr<SpillableState> state;
        uint64_t coldness;
    };

    std::vector<Candidate> candidates;
    size_t resident = 0;
    {
        const std::shared_lock lock(registryMutex);
        candidates.reserve(registry.size());
        for (const auto& [ptr, entry] : registry)
        {
            if (auto state = entry->state.lock())
            {
                resident += state->residentBytes();
                candidates.push_back({entry, std::move(state), 0});
            }
        }
    }
    if (resident <= targetBytes)
    {
        return 0;
    }
    for (auto& candidate : candidates)
    {
        candidate.coldness = candidate.state->coldnessKey();
    }

    /// Coldest first (smallest coldnessKey is evicted first).
    std::ranges::sort(candidates, [](const auto& lhs, const auto& rhs) { return lhs.coldness < rhs.coldness; });

    size_t freed = 0;
    for (const auto& candidate : candidates)
    {
        if (resident - freed <= targetBytes)
        {
            break;
        }
        const std::lock_guard guard(candidate.entry->residencyMutex);
        /// Re-check under the residency mutex: skip pinned or already-evicted units.
        if (candidate.entry->pinCount.load() != 0 || candidate.state->isEvicted())
        {
            continue;
        }
        const auto bytes = candidate.state->residentBytes();
        candidate.state->evictState();
        freed += bytes;
    }
    return freed;
}

}
