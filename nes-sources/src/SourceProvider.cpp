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

#include <Sources/SourceProvider.hpp>

#include <memory>
#include <string>
#include <utility>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include <fmt/format.h>

#include <Async/AsyncSourceHandle.hpp>
#include <Blocking/BlockingSourceHandle.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Util/CcxTopology.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>

namespace NES
{

SourceProvider::SourceProvider(
    const size_t defaultMaxInflightBuffers,
    std::shared_ptr<AbstractBufferProvider> bufferPool,
    const bool ccxSharedSourcePools,
    const size_t ccxSourcePoolBuffers)
    : defaultMaxInflightBuffers(defaultMaxInflightBuffers), bufferPool(std::move(bufferPool)), ccxSourcePoolBuffers(ccxSourcePoolBuffers)
{
    if (ccxSharedSourcePools)
    {
        /// Requires the CCX cell layout (ccx_aware_task_queues + pin_threads, striped): the
        /// QueryEngine is constructed before the SourceProvider, so shardingEnabled/topology
        /// are already settled here.
        if (!CcxAffinity::stripedLayout())
        {
            NES_WARNING("ccx_shared_source_pools is enabled without the CCX-striped cell layout "
                        "(needs ccx_aware_task_queues + pin_threads); sources keep private pools.");
        }
        else
        {
            const auto numCcx = CcxTopology::instance().numCcx();
            for (size_t ccx = 0; ccx < numCcx; ++ccx)
            {
                cellPools.push_back(BufferManager::create(
                    static_cast<uint32_t>(this->bufferPool->getBufferSize()),
                    static_cast<uint32_t>(ccxSourcePoolBuffers),
                    std::make_shared<NesDefaultMemoryAllocator>(),
                    static_cast<uint32_t>(std::max<size_t>(this->bufferPool->getBufferAlignment(), 64))));
            }
            NES_INFO(
                "ccx_shared_source_pools: created {} cell pools of {} x {} B ({} MB per cell)",
                numCcx,
                ccxSourcePoolBuffers,
                this->bufferPool->getBufferSize(),
                (ccxSourcePoolBuffers * this->bufferPool->getBufferSize()) >> 20);

            /// TMP DIAGNOSTIC (NES_CELLPOOL_STATS=1): print each cell pool's available buffer
            /// count every 2 s + once on shutdown, to catch pool depletion across query
            /// lifecycles (N=1024 straggler-stall investigation). REVERT before merge.
            if (std::getenv("NES_CELLPOOL_STATS") != nullptr)
            {
                cellPoolStatsThread = std::jthread(
                    [pools = cellPools, total = ccxSourcePoolBuffers](const std::stop_token& stopToken)
                    {
                        const auto printLine = [&pools, total](const char* tag)
                        {
                            std::string counts;
                            for (const auto& pool : pools)
                            {
                                counts += fmt::format(" {}", pool->getNumberOfAvailableBuffers());
                            }
                            std::fprintf(stderr, "DIAG_CELLPOOL%s avail%s /%zu\n", tag, counts.c_str(), total);
                        };
                        while (!stopToken.stop_requested())
                        {
                            printLine("");
                            for (int i = 0; i < 20 && !stopToken.stop_requested(); ++i)
                            {
                                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                            }
                        }
                        printLine("_FINAL");
                    });
            }
        }
    }
}

std::unique_ptr<SourceHandle> SourceProvider::lower(
    OriginId originId,
    BackpressureListener backpressureListener,
    const SourceDescriptor& sourceDescriptor,
    const bool pinThreads,
    const size_t numberOfIOThreads) const
{
    /// Todo #241: Get the new source identfier from the source descriptor and pass it to SourceHandle.
    auto sourceArguments = SourceRegistryArguments(sourceDescriptor);
    if (auto source = SourceRegistry::instance().create(sourceDescriptor.getSourceType(), sourceArguments))
    {
        /// The source-specific configuration of maxInflightBuffers takes priority.
        /// If not specified (0), we take the NodeEngine-wide configuration.
        const bool hasInflightOverride = sourceDescriptor.getFromConfig(SourceDescriptor::MAX_INFLIGHT_BUFFERS) > 0;
        auto maxInflightBuffers
            = hasInflightOverride ? sourceDescriptor.getFromConfig(SourceDescriptor::MAX_INFLIGHT_BUFFERS) : defaultMaxInflightBuffers;
        const auto inputFormatterThreadingMode = sourceDescriptor.getInputFormatterDescriptor().getThreadingMode();

        /// One slot counter drives the async io-context choice, the blocking-thread pin AND the
        /// cell-pool choice below, so a source's pool always matches its io thread's CCX.
        const size_t ioSlot = nextSourceSlot.fetch_add(1, std::memory_order_relaxed) % std::max<size_t>(1, numberOfIOThreads);

        /// Prefill sources (io_uring variants, InMemory prefill) own their buffers differently
        /// (deep-QD priming against pool depth; prefilled stashes) and keep private pools.
        const bool preFills = std::visit([](const auto& sourceImpl) { return sourceImpl->preFillsBuffers(); }, source.value());

        std::shared_ptr<AbstractBufferProvider> sourcePool;
        if (!cellPools.empty() && CcxAffinity::stripedLayout() && !preFills && !hasInflightOverride)
        {
            /// Shared per-CCX cell pool: buffers cycle within one cell's L3 (io feed -> shard ->
            /// owner workers -> recycle). The per-source inflight semaphore stays the fairness
            /// cap; clamp it to half the cell pool so no single source can monopolize it. NOTE:
            /// no per-source reserved minimum -- a backlogged source can transiently starve cell
            /// peers (never deadlock: held buffers return via worker task completion).
            const auto ccx = CcxTopology::instance().ccxOf(CcxTopology::instance().ioThreadCpu(ioSlot));
            sourcePool = cellPools[std::min<size_t>(ccx, cellPools.size() - 1)];
            maxInflightBuffers = std::min<size_t>(maxInflightBuffers, std::max<size_t>(1, ccxSourcePoolBuffers / 2));
        }
        else
        {
            /// Give the source its OWN buffer pool sized to the inflight limit, instead of the shared global pool.
            /// The pool size IS the backpressure: the source blocks in getBuffer once all its buffers are inflight
            /// (the same bound the inflight semaphore enforces), and a small pool that is reused over and over keeps
            /// its pages faulted/warm -- avoiding the first-touch page faults that otherwise dominate cold reads from
            /// a large pool (HANDOVER 2_engine_to_e2e §41/§43). Pipeline OUTPUTS still come from the global pool (the
            /// worker pec), so a small source pool can never starve them. Match the global pool's buffer size and
            /// alignment so io_uring registered-buffer O_DIRECT (which needs device-block aligned payloads) works on
            /// this pool too when global_buffer_alignment is raised.
            sourcePool = BufferManager::create(
                static_cast<uint32_t>(bufferPool->getBufferSize()),
                static_cast<uint32_t>(maxInflightBuffers),
                std::make_shared<NesDefaultMemoryAllocator>(),
                static_cast<uint32_t>(std::max<size_t>(bufferPool->getBufferAlignment(), 64)));
        }

        SourceRuntimeConfiguration runtimeConfig{
            .inflightBufferLimit = maxInflightBuffers, .inputFormatterThreadingMode = inputFormatterThreadingMode};

        return std::visit(
            Overloaded{
                [&](std::unique_ptr<BlockingSource>&& sourceImpl) -> std::unique_ptr<SourceHandle>
                {
                    return std::make_unique<BlockingSourceHandle>(
                        std::move(backpressureListener),
                        std::move(originId),
                        std::move(runtimeConfig),
                        std::move(sourcePool),
                        std::move(sourceImpl),
                        pinThreads,
                        numberOfIOThreads,
                        ioSlot);
                },
                [&](std::unique_ptr<AsyncSource>&& sourceImpl) -> std::unique_ptr<SourceHandle>
                {
                    return std::make_unique<AsyncSourceHandle>(
                        std::move(backpressureListener),
                        std::move(originId),
                        std::move(runtimeConfig),
                        std::move(sourcePool),
                        std::move(sourceImpl),
                        pinThreads,
                        numberOfIOThreads,
                        ioSlot);
                }},
            std::move(source.value()));
    }
    // return std::make_unique<SourceHandle>(
    //     std::move(backpressureListener), std::move(originId), std::move(runtimeConfig), bufferPool, std::move(source.value()));
    throw UnknownSourceType("unknown source descriptor type: {}", sourceDescriptor.getSourceType());
}

bool SourceProvider::contains(const std::string& sourceType) const ///NOLINT(readability-convert-member-functions-to-static)
{
    return SourceRegistry::instance().contains(sourceType);
}

}
