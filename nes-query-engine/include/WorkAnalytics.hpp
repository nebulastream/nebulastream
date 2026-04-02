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
#include <fstream>
#include <string>
#include <vector>

namespace NES
{

/// Per-thread counters for analyzing work dealing and stealing behavior.
/// All counters are thread-local (only incremented by the owning thread) except
/// stolenByOther which is incremented by stealing threads and thus uses an atomic.
struct alignas(64) ThreadWorkCounters
{
    /// Tasks read from the shared admission queue
    uint64_t admissionTaken{0};

    /// Tasks read from own per-thread queue (internal/successor tasks)
    uint64_t localTaken{0};

    /// Tasks stolen from another thread's queue (this thread is the thief)
    uint64_t stolenFromOther{0};

    /// Successor tasks sent to another thread's queue via dealing strategy
    uint64_t dealtToOther{0};

    /// Successor tasks kept on own queue (producer-local)
    uint64_t dealtToSelf{0};

    /// Times the worker found no task (semaphore timeout or empty queues)
    uint64_t idleCycles{0};

    /// Total successor tasks produced by this thread
    uint64_t successorsProduced{0};

    /// Tasks stolen FROM this thread's queue by other threads (victim counter).
    /// Atomic because multiple stealing threads may increment concurrently.
    std::atomic<uint64_t> stolenByOther{0};
};

/// Collection of per-thread work counters with CSV export.
class WorkAnalytics
{
    std::vector<ThreadWorkCounters> counters;
    bool enabled_;

public:
    explicit WorkAnalytics(size_t numThreads, bool enabled = false)
        : counters(numThreads), enabled_(enabled)
    {
    }

    [[nodiscard]] bool isEnabled() const { return enabled_; }

    /// Get the counters for a specific thread. Only the owning thread should
    /// write to non-atomic fields; any thread may increment stolenByOther.
    ThreadWorkCounters& operator[](size_t threadIndex) { return counters[threadIndex]; }
    const ThreadWorkCounters& operator[](size_t threadIndex) const { return counters[threadIndex]; }

    [[nodiscard]] size_t numThreads() const { return counters.size(); }

    /// Export all counters to a CSV file.
    void exportCSV(const std::string& path) const
    {
        std::ofstream out(path);
        out << "thread_id,admission_taken,local_taken,stolen_from_other,dealt_to_other,"
               "dealt_to_self,idle_cycles,successors_produced,stolen_by_other,total_processed\n";
        for (size_t i = 0; i < counters.size(); ++i)
        {
            const auto& c = counters[i];
            const auto totalProcessed = c.admissionTaken + c.localTaken + c.stolenFromOther;
            out << i << ','
                << c.admissionTaken << ','
                << c.localTaken << ','
                << c.stolenFromOther << ','
                << c.dealtToOther << ','
                << c.dealtToSelf << ','
                << c.idleCycles << ','
                << c.successorsProduced << ','
                << c.stolenByOther.load(std::memory_order_relaxed) << ','
                << totalProcessed << '\n';
        }
    }
};

}
