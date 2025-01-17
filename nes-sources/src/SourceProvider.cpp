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
#include <memory>
#include <utility>
#include <variant>

#include <Async/AsyncSourceRunner.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceRunner.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>

namespace NES::Sources
{

std::unique_ptr<SourceProvider> SourceProvider::create()
{
    return std::make_unique<SourceProvider>();
}

std::unique_ptr<SourceRunner> SourceProvider::lower(
    OriginId originId, const SourceDescriptor& sourceDescriptor, std::shared_ptr<Memory::AbstractPoolProvider> poolProvider)
{
    auto inputFormatter = InputFormatters::InputFormatterProvider::provideInputFormatter(
        sourceDescriptor.parserConfig.parserType,
        sourceDescriptor.schema,
        sourceDescriptor.parserConfig.tupleDelimiter,
        sourceDescriptor.parserConfig.fieldDelimiter);

    auto sourceArguments = SourceRegistryArguments(sourceDescriptor);
    if (auto source = SourceRegistry::instance().create(sourceDescriptor.sourceType, sourceArguments))
    {
        if (auto bufferProvider = poolProvider->createFixedSizeBufferPool(NUM_SOURCE_LOCAL_BUFFERS); bufferProvider)
        {
            auto sourceContext = SourceExecutionContext{originId, *bufferProvider, std::move(inputFormatter)};
            if (std::holds_alternative<std::unique_ptr<AsyncSource>>(*source))
            {
                auto asyncContext = AsyncSourceExecutionContext{(std::move(std::get<std::unique_ptr<AsyncSource>>(*source)))};
                return std::make_unique<AsyncSourceRunner>(std::move(sourceContext), std::move(asyncContext));
            }
            else /// if (std::holds_alternative<BlockingSource>(*source))
            {
                return nullptr;
            }
        }
        throw CannotAllocateBuffer();
    }
    throw UnknownSourceType("unknown source descriptor type: {}", sourceDescriptor.sourceType);
}

}