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
#include <cstdint>

#include <Async/AsyncSourceHandle.hpp>
#include <Blocking/BlockingSourceHandle.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Util/Overloaded.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>

namespace NES
{

SourceProvider::SourceProvider(size_t defaultMaxInflightBuffers, std::shared_ptr<AbstractBufferProvider> bufferPool)
    : defaultMaxInflightBuffers(defaultMaxInflightBuffers), bufferPool(std::move(bufferPool))
{
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
        const auto maxInflightBuffers = (sourceDescriptor.getFromConfig(SourceDescriptor::MAX_INFLIGHT_BUFFERS) > 0)
            ? sourceDescriptor.getFromConfig(SourceDescriptor::MAX_INFLIGHT_BUFFERS)
            : defaultMaxInflightBuffers;
        const auto inputFormatterThreadingMode = sourceDescriptor.getInputFormatterDescriptor().getThreadingMode();
        SourceRuntimeConfiguration runtimeConfig{
            .inflightBufferLimit = maxInflightBuffers, .inputFormatterThreadingMode = inputFormatterThreadingMode};

        /// Give the source its OWN buffer pool sized to the inflight limit, instead of the shared global pool.
        /// The pool size IS the backpressure: the source blocks in getBuffer once all its buffers are inflight
        /// (the same bound the inflight semaphore enforces), and a small pool that is reused over and over keeps
        /// its pages faulted/warm -- avoiding the first-touch page faults that otherwise dominate cold reads from
        /// a large pool (HANDOVER 2_engine_to_e2e §41/§43). Pipeline OUTPUTS still come from the global pool (the
        /// worker pec), so a small source pool can never starve them. Match the global pool's buffer size and
        /// alignment so io_uring registered-buffer O_DIRECT (which needs device-block aligned payloads) works on
        /// this pool too when global_buffer_alignment is raised.
        auto sourcePool = BufferManager::create(
            static_cast<uint32_t>(bufferPool->getBufferSize()),
            static_cast<uint32_t>(maxInflightBuffers),
            std::make_shared<NesDefaultMemoryAllocator>(),
            static_cast<uint32_t>(std::max<size_t>(bufferPool->getBufferAlignment(), 64)));

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
                        numberOfIOThreads);
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
                        numberOfIOThreads);
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
