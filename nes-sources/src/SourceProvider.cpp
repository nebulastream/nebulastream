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
#include <stdexcept>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Sources/Registry/SourceRegistry.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/TCPSource.hpp>

namespace NES::Sources
{

DataSourceProviderPtr SourceProvider::create()
{
    return std::make_shared<SourceProvider>();
}

SourceHandlePtr SourceProvider::lower(
    OriginId originId,
    std::unique_ptr<SourceDescriptor>&& sourceDescriptor,
    std::shared_ptr<NES::Runtime::AbstractPoolProvider> bufferPool,
    SourceReturnType::EmitFunction&& emitFunction)
{
    auto schema = sourceDescriptor->getSchema();
    /// Todo #241: Get the new source identfier from the source descriptor and pass it to SourceHandle.
    if (auto source = SourceRegistry::instance().tryCreate(sourceDescriptor->getSourceName(), schema, std::move(sourceDescriptor));
        source.has_value())
    {
        return std::make_shared<SourceHandle>(
            std::move(originId),
            std::move(schema),
            std::move(bufferPool),
            std::move(emitFunction),
            NUM_SOURCE_LOCAL_BUFFERS,
            std::move(source.value()));
    }
    NES_ERROR("ConvertLogicalToPhysicalSource: Unknown Source Descriptor Type {}", schema->toString());
    throw std::invalid_argument("Unknown Source Descriptor Type");
}

}
