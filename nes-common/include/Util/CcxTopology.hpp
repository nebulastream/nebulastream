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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

namespace NES
{

/// Groups the machine's logical CPUs by shared last-level cache (an AMD "CCX"; one group on
/// monolithic-L3 CPUs). Used by the CCX-aware task queue sharding: threads that share an L3
/// exchange buffers through it, threads on different CCXs pay a cross-fabric RFO per cache line.
///
/// Topology source precedence: NES_CCX_TOPOLOGY env var > explicit string (setOverride, from the
/// worker config) > sysfs auto-detect > single-CCX fallback. The string format is one group per
/// ';', each group a comma-separated list of cpu ids and ranges, e.g. "0-5,24-29;6-11,30-35".
class CcxTopology
{
public:
    /// Process-wide instance. The first call materializes the topology; setOverride must happen before.
    static const CcxTopology& instance();

    /// Installs a config-provided topology string (empty = keep auto-detection). Must be called
    /// before the first instance() call to take effect; later calls are ignored with a warning.
    static void setOverride(std::string_view topology);

    /// Parses the override format. Throws InvalidConfigParameter on malformed input.
    static CcxTopology fromString(std::string_view topology);

    /// Reads /sys/devices/system/cpu/cpu*/cache/index3/shared_cpu_list. Lists include SMT
    /// siblings ("0-5,24-29"); groups are deduplicated by their literal list string and numbered
    /// in first-seen (ascending cpu id) order, so CCX0 is cpu0's group. Any read/parse failure
    /// degrades to a single CCX (with a warning): sharding then degenerates to one queue.
    static CcxTopology detectFromSysfs();

    [[nodiscard]] uint32_t ccxOf(size_t cpu) const { return cpu < cpuToCcx.size() ? cpuToCcx[cpu] : 0; }

    [[nodiscard]] size_t numCcx() const { return ccxCount; }

    /// CCX-STRIPED thread layout (used when ccx_aware_task_queues is on): io-role threads are
    /// distributed round-robin across the CCXs (4 io threads on a 4-CCX part = 1 per CCX, 8 = 2
    /// per CCX), and workers are striped the same way into each CCX's remaining cpu slots. Every
    /// CCX then forms a self-contained cell -- its io thread(s) feed its admission shard, owned
    /// by its co-located workers -- and the round-robin source->io-context assignment spreads
    /// sources across the cells. Group cpu lists come from sysfs in ascending order, so physical
    /// cores fill before SMT siblings.
    [[nodiscard]] size_t ioThreadCpu(size_t ioIndex) const;
    [[nodiscard]] size_t workerCpu(size_t workerIndex, size_t numIoThreads) const;

private:
    CcxTopology() = default;
    explicit CcxTopology(const std::vector<std::vector<size_t>>& groups);

    std::vector<uint32_t> cpuToCcx;
    std::vector<std::vector<size_t>> ccxGroups{{0}};
    size_t ccxCount{1};
};

/// Thread-local CCX tag, mirroring WorkerThread::id: set once right after a thread pins itself
/// (io threads, blocking-source threads, workers). Emitting threads use it to pick the admission
/// shard of their own CCX; untagged (unpinned) threads fall back to shard 0.
namespace CcxAffinity
{
inline constexpr uint32_t UNKNOWN = std::numeric_limits<uint32_t>::max();
inline thread_local uint32_t ccxId = UNKNOWN; /// NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

/// Set by the QueryEngine (before any source starts) when ccx_aware_task_queues is enabled.
/// Gates behavior that only makes sense under sharding -- notably pinning blocking-source
/// threads, which stay unpinned in the default configuration (kill-switch fidelity).
inline std::atomic<bool> shardingEnabled{false}; /// NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

/// True when pinned threads should use the CCX-striped layout (see CcxTopology::ioThreadCpu):
/// sharding is on and the experiment escape hatch NES_CCX_PIN_LAYOUT=compact is not set.
inline bool stripedLayout()
{
    if (!shardingEnabled.load(std::memory_order_relaxed))
    {
        return false;
    }
    const char* layout = std::getenv("NES_CCX_PIN_LAYOUT");
    return layout == nullptr || std::string_view{layout} != "compact";
}

inline uint32_t shardOrDefault(const size_t numShards)
{
    return ccxId == UNKNOWN ? 0U : static_cast<uint32_t>(std::min<size_t>(ccxId, numShards - 1));
}
}

}
