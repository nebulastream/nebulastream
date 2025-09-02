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
#include <variant>

#include <Async/AsyncSourceHandle.hpp>
#include <Blocking/BlockingSourceHandle.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/BlockingSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>

namespace NES::Sources
{

std::unique_ptr<SourceHandle> SourceProvider::lower(
    const OriginId originId,
    const SourceDescriptor& sourceDescriptor,
    std::shared_ptr<Memory::AbstractPoolProvider> poolProvider,
    const size_t numBuffersPerSource)
{
    auto sourceArguments = SourceRegistryArguments(sourceDescriptor);
    const auto numberOfBuffersInLocalPool = (sourceDescriptor.getFromConfig(SourceDescriptor::NUMBER_OF_BUFFERS_IN_LOCAL_POOL) > 0)
        ? sourceDescriptor.getFromConfig(SourceDescriptor::NUMBER_OF_BUFFERS_IN_LOCAL_POOL)
        : numBuffersPerSource;
    if (auto source = SourceRegistry::instance().create(sourceDescriptor.getSourceType(), sourceArguments))
    {
        if (const auto bufferProvider = poolProvider->createFixedSizeBufferPool(numberOfBuffersInLocalPool); bufferProvider)
        {
            return std::visit(
                Overloaded{
                    [&](std::unique_ptr<BlockingSource>&& sourceImpl) -> std::unique_ptr<SourceHandle>
                    {
                        const auto formattingThread = sourceDescriptor.getFromConfig(SourceDescriptor::FORMATTING_THREAD);
                        return std::make_unique<BlockingSourceHandle>(SourceExecutionContext{
                            .originId = originId,
                            .sourceImpl = std::move(sourceImpl),
                            .bufferProvider = bufferProvider.value(),
                            .formattingThread = sourceDescriptor.getFromConfig(SourceDescriptor::FORMATTING_THREAD)});
                    },
                    [&](std::unique_ptr<AsyncSource>&& sourceImpl) -> std::unique_ptr<SourceHandle>
                    {
                        return std::make_unique<AsyncSourceHandle>(SourceExecutionContext{
                            .originId = originId,
                            .sourceImpl = std::move(sourceImpl),
                            .bufferProvider = bufferProvider.value(),
                            .formattingThread = std::nullopt});
                    }},
                std::move(source.value()));
        }
        throw BufferAllocationFailure(
            "Cannot allocate the buffer pool for source: {}", sourceDescriptor.getLogicalSource().getLogicalSourceName());
    }
    throw UnknownSourceType("Unknown source descriptor type: {}", sourceDescriptor.getSourceType());
}

}