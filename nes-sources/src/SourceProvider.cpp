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

namespace NES
{

SourceProvider::SourceProvider(size_t defaultMaxInflightBuffers, std::shared_ptr<AbstractBufferProvider> bufferPool)
    : defaultMaxInflightBuffers(defaultMaxInflightBuffers), bufferPool(std::move(bufferPool))
{
}

std::unique_ptr<SourceHandle> SourceProvider::lower(const OriginId originId, const SourceDescriptor& sourceDescriptor) const
{
    auto sourceArguments = SourceRegistryArguments(sourceDescriptor);
    if (auto source = SourceRegistry::instance().create(sourceDescriptor.getSourceType(), sourceArguments))
    {
        return std::visit(
            Overloaded{
                [&](std::unique_ptr<BlockingSource>&& sourceImpl) -> std::unique_ptr<SourceHandle>
                {
                    const auto formattingThread = sourceDescriptor.getFromConfig(SourceDescriptor::FORMATTING_THREAD);
                    return std::make_unique<BlockingSourceHandle>(
                        SourceExecutionContext{
                            .originId = originId,
                            .sourceImpl = std::move(sourceImpl),
                            .bufferProvider = bufferPool,
                            .formattingThread = sourceDescriptor.getFromConfig(SourceDescriptor::FORMATTING_THREAD)},
                        defaultMaxInflightBuffers);
                },
                [&](std::unique_ptr<AsyncSource>&& sourceImpl) -> std::unique_ptr<SourceHandle>
                {
                    return std::make_unique<AsyncSourceHandle>(
                        SourceExecutionContext{
                            .originId = originId,
                            .sourceImpl = std::move(sourceImpl),
                            .bufferProvider = bufferPool,
                            .formattingThread = std::nullopt},
                        defaultMaxInflightBuffers);
                }},
            std::move(source.value()));
    }
    throw UnknownSourceType("Unknown source descriptor type: {}", sourceDescriptor.getSourceType());
}

}