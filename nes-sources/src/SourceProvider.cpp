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

#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/AsyncSourceExecutor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceReturnType.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>

namespace NES::Sources
{

std::unique_ptr<SourceProvider> SourceProvider::create()
{
    return std::make_unique<SourceProvider>();
}

std::unique_ptr<SourceHandle> SourceProvider::lower(
    OriginId originId,
    const SourceDescriptor& sourceDescriptor,
    std::shared_ptr<Memory::AbstractPoolProvider> bufferPool,
    SourceReturnType::EmitFunction&& emitFunction)
{
    /// Todo #241: Get the new source identfier from the source descriptor and pass it to SourceHandle.
    /// Todo #495: If we completely move the InputFormatter out of the sources, we get rid of constructing the parser here.
    auto inputFormatter = NES::InputFormatters::InputFormatterProvider::provideInputFormatter(
        sourceDescriptor.parserConfig.parserType,
        sourceDescriptor.schema,
        sourceDescriptor.parserConfig.tupleDelimiter,
        sourceDescriptor.parserConfig.fieldDelimiter);

    auto sourceArguments = NES::Sources::SourceRegistryArguments(sourceDescriptor);
    if (auto source = SourceRegistry::instance().create(sourceDescriptor.sourceType, sourceArguments))
    {
        return std::make_unique<SourceHandle>(
            originId,
            std::move(bufferPool),
            std::move(emitFunction),
            NUM_SOURCE_LOCAL_BUFFERS,
            std::move(source.value()),
            std::move(inputFormatter));
    }
    throw UnknownSourceType("unknown source descriptor type: {}", sourceDescriptor.sourceType);
}

}
