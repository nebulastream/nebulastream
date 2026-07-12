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
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <BackpressureChannel.hpp>

namespace NES
{

/// Takes a SourceDescriptor and in exchange returns a SourceHandle.
/// The SourceThread spawns an independent thread for data ingestion and it manages the pipeline and task logic.
/// The Source is owned by the SourceThread. The Source ingests bytes from an interface (TCP, CSV, ..) and writes the bytes to a TupleBuffer.
class SourceProvider
{
    size_t defaultMaxInflightBuffers;
    std::shared_ptr<AbstractBufferProvider> bufferPool;

    /// Per-CCX shared source pools (worker.ccx_shared_source_pools; empty = feature off): all
    /// non-prefill sources whose io threads land on one CCX draw from ONE pool, capping the
    /// recycled-ring working set at numCcx x ccx_source_pool_buffers regardless of source count
    /// (per-source 8 MB rings spill the L3 at >=16 sources and halve the warm supply rate).
    /// The pools outlive every source: SourceProvider is a NodeEngine member destroyed after
    /// the QueryEngine (which stops all sources) and before the global pool.
    std::vector<std::shared_ptr<BufferManager>> cellPools;
    size_t ccxSourcePoolBuffers;
    /// One counter drives the io-context choice, the blocking-thread pin slot AND the cell-pool
    /// choice, so a source's pool always matches the CCX its io thread runs on. mutable: lower()
    /// is const at every call site.
    mutable std::atomic<size_t> nextSourceSlot{0};

    /// TMP DIAGNOSTIC (NES_CELLPOOL_STATS=1): periodically prints each cell pool's available
    /// buffer count to stderr (DIAG_CELLPOOL), to catch pool depletion over query lifecycles
    /// (N=1024 straggler-stall investigation). Declared after cellPools so it joins before the
    /// pools are destroyed. REVERT before merge.
    std::jthread cellPoolStatsThread;

public:
    /// Constructor that can be configured with various options
    SourceProvider(
        size_t defaultMaxInflightBuffers,
        std::shared_ptr<AbstractBufferProvider> bufferPool,
        bool ccxSharedSourcePools = false,
        size_t ccxSourcePoolBuffers = 128);

    /// Returning a shared pointer, because sources may be shared by multiple executable query plans (qeps).
    [[nodiscard]] std::unique_ptr<SourceHandle> lower(
        OriginId originId,
        BackpressureListener backpressureListener,
        const SourceDescriptor& sourceDescriptor,
        bool pinThreads,
        size_t numberOfIOThreads) const;

    [[nodiscard]] bool contains(const std::string& sourceType) const;
};

}
