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
#include <cstddef>
#include <memory>
#include <utility>
#include <variant>

#include <Async/AsyncSourceHandle.hpp>
#include <Blocking/BlockingSourceHandle.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
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

std::unique_ptr<SourceProvider> SourceProvider::create()
{
    return std::make_unique<SourceProvider>();
}

std::unique_ptr<SourceHandle> SourceProvider::lower(
    const OriginId originId,
    const SourceDescriptor& sourceDescriptor,
    std::shared_ptr<Memory::AbstractPoolProvider> poolProvider,
    const size_t numBuffersPerSource)
{
    auto inputFormatter = InputFormatters::InputFormatterProvider::provideInputFormatter(
        sourceDescriptor.parserConfig.parserType,
        sourceDescriptor.schema,
        sourceDescriptor.parserConfig.tupleDelimiter,
        sourceDescriptor.parserConfig.fieldDelimiter);

    auto sourceArguments = SourceRegistryArguments(sourceDescriptor);
    if (auto source = SourceRegistry::instance().create(sourceDescriptor.sourceType, sourceArguments))
    {
        if (const auto bufferProvider = poolProvider->createFixedSizeBufferPool(numBuffersPerSource); bufferProvider)
        {
            return std::visit(
                Overloaded{
                    [&](std::unique_ptr<BlockingSource>&& sourceImpl) -> std::unique_ptr<SourceHandle>
                    {
                        return std::make_unique<BlockingSourceHandle>(SourceExecutionContext{
                            .originId = originId,
                            .sourceImpl = std::move(sourceImpl),
                            .bufferProvider = bufferProvider.value(),
                            .inputFormatter = std::move(inputFormatter)});
                    },
                    [&](std::unique_ptr<AsyncSource>&& sourceImpl) -> std::unique_ptr<SourceHandle>
                    {
                        return std::make_unique<AsyncSourceHandle>(SourceExecutionContext{
                            .originId = originId,
                            .sourceImpl = std::move(sourceImpl),
                            .bufferProvider = bufferProvider.value(),
                            .inputFormatter = std::move(inputFormatter)});
                    }},
                std::move(source.value()));
        }
        throw BufferAllocationFailure("Cannot allocate the buffer pool for source: {}", sourceDescriptor.logicalSourceName);
    }
    throw UnknownSourceType("Unknown source descriptor type: {}", sourceDescriptor.sourceType);
}

}