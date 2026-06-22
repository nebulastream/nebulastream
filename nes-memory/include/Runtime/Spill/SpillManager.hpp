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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <Runtime/Spill/ArenaMemoryResource.hpp>

namespace NES
{

/// Interface a spillable unit of operator state (a window/aggregation/join slice) implements so the process-wide
/// SpillManager can account for its DRAM footprint and evict/reload it under memory pressure. Implementations back
/// their state with an ArenaMemoryResource so eviction preserves virtual addresses (see ArenaMemoryResource).
class SpillableState
{
public:
    virtual ~SpillableState() = default;

    /// Physical DRAM footprint of this unit right now: its byte size when resident, 0 when evicted.
    [[nodiscard]] virtual size_t residentBytes() const = 0;
    [[nodiscard]] virtual bool isEvicted() const = 0;

    /// Move this unit's state out of DRAM. Called by the SpillManager only while unpinned and resident.
    virtual void evictState() = 0;
    /// Bring this unit's state back into DRAM at its original addresses. Called before the unit is pinned for access.
    virtual void reloadState() = 0;

    /// Victim-selection ordering: a smaller key is colder and is evicted first. For time-based slices this is the
    /// slice end (oldest slice = least likely to be written next under monotone event time).
    [[nodiscard]] virtual uint64_t coldnessKey() const = 0;
};

/// Policy for the SpillManager. Budgets are in bytes; watermarks are fractions of the budget.
struct SpillConfiguration
{
    static constexpr double DEFAULT_HIGH_WATERMARK = 0.9;
    static constexpr double DEFAULT_LOW_WATERMARK = 0.7;

    bool enabled = false;
    /// Target ceiling for total resident state bytes across all registered units. 0 => no proactive eviction.
    size_t stateMemoryBudgetBytes = 0;
    /// Start evicting once resident bytes exceed highWatermark * budget ...
    double highWatermark = DEFAULT_HIGH_WATERMARK;
    /// ... and keep evicting the coldest unpinned units down to lowWatermark * budget.
    double lowWatermark = DEFAULT_LOW_WATERMARK;
    /// Directory under which per-slice arena backing files are created.
    std::string spillDirectory = "/tmp";
    /// Backend used by per-slice arenas (Anon = vmcache-faithful explicit pwrite/madvise/pread; the production default).
    ArenaMemoryResource::Mode arenaMode = ArenaMemoryResource::Mode::Anon;
};

/// Process-wide governor that decides when and what to spill. Operators do not evict their own state; they only
/// register/unregister their slices and pin/unpin them around access. The governor owns the eviction policy so the
/// budget is global across operators (a join can evict an aggregation's cold slice and vice versa).
///
/// Thread-safety: the registry is guarded by a shared mutex; each unit has its own residency mutex. Eviction selects
/// victims under a brief registry read-lock (copying shared_ptrs) and performs the actual evict I/O outside that lock,
/// under the per-unit residency mutex, so disk I/O never serializes the whole registry.
class SpillManager
{
public:
    /// Non-copyable and non-movable (the std::shared_mutex member makes it so implicitly).
    explicit SpillManager(SpillConfiguration configuration);

    /// Register a unit so the governor accounts for and may evict it. Safe to call from any thread.
    void registerState(const std::shared_ptr<SpillableState>& state);
    /// Stop tracking a unit (e.g. on slice garbage collection). Idempotent.
    void unregisterState(const SpillableState* state);

    /// Reload-if-evicted and increment the unit's pin count. While pinned (>0) the unit is never evicted. Must be
    /// called before a worker writes or probes the unit's state.
    void pin(SpillableState& state);
    /// Decrement the unit's pin count; the unit becomes evictable again when it reaches 0.
    void unpin(SpillableState& state);

    /// Proactive eviction: if resident bytes exceed the high watermark, evict the coldest unpinned units until resident
    /// bytes drop to the low watermark (or no more evictable units remain). Returns the number of bytes evicted.
    size_t maybeSpill();

    /// Reactive backstop: evict coldest unpinned units until resident bytes <= targetBytes (or none remain). Returns
    /// bytes evicted. Used to make room before an allocation that would otherwise breach the budget.
    size_t evictDownTo(size_t targetBytes);

    /// Sum of residentBytes() across all registered units.
    [[nodiscard]] size_t residentBytes() const;
    [[nodiscard]] size_t registeredCount() const;

    [[nodiscard]] const SpillConfiguration& configuration() const noexcept { return config; }

private:
    /// Per-unit bookkeeping. Stored behind a shared_ptr so its address (and mutex) stay stable across registry rehash.
    /// The state is held by weak_ptr: the SliceStore owns the slice's lifetime, so the governor must not keep it alive.
    /// Eviction/pin lock the weak_ptr to a temporary shared_ptr, which keeps the unit alive for the duration of the I/O
    /// and returns null (skip) if the unit is concurrently being destroyed.
    struct Entry
    {
        std::weak_ptr<SpillableState> state;
        std::atomic<uint32_t> pinCount{0};
        std::mutex residencyMutex; /// serializes evict/reload and pin-reload for this unit
    };

    std::shared_ptr<Entry> lookup(const SpillableState* state) const;

    SpillConfiguration config;
    mutable std::shared_mutex registryMutex;
    std::map<const SpillableState*, std::shared_ptr<Entry>> registry;
};

}
