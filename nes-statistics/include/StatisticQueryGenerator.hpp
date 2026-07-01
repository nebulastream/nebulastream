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
#include <string>
#include <Plans/LogicalPlan.hpp>

namespace NES
{

/// Generates the data-plane queries the StatisticCoordinator submits.
/// Lean PoC: both queries are driven by a generator heartbeat, shape their tuples with a projection of
/// constants, run through the scalar StatisticStore write/read operators, and ship results over a FIFO
/// via a FileSink. See DefaultStatisticQueryGenerator for the concrete plan shapes.
class StatisticQueryGenerator
{
public:
    virtual ~StatisticQueryGenerator() = default;

    /// Collection query: populates the store with (statisticId, 0, windowSizeMs, value) and notifies the
    /// coordinator over the FIFO at `fifoPath`.
    [[nodiscard]] virtual LogicalPlan
    generateBuildQuery(uint64_t statisticId, uint64_t windowSizeMs, const std::string& fifoPath) const = 0;

    /// Probe query (generated per getStatistics call): reads (statisticId, startTs, endTs) from the store and
    /// ships the value over the FIFO at `fifoPath`. Keys are baked in as constants (no request channel).
    [[nodiscard]] virtual LogicalPlan
    generateProbeQuery(uint64_t statisticId, uint64_t startTs, uint64_t endTs, const std::string& fifoPath) const = 0;

    /// Combined watch query (continuous, no separate probe): the writer puts (statisticId, 0, windowSizeMs, value)
    /// into the store and forwards the record as an impulse to the reader, which reads the value back out of the
    /// store and ships it over the FIFO. The generator is throttled so each impulse (one emitted tuple, standing in
    /// for one completed window) produces an observable, periodic result.
    [[nodiscard]] virtual LogicalPlan
    generateWatchQuery(uint64_t statisticId, uint64_t windowSizeMs, const std::string& fifoPath) const = 0;
};

}
