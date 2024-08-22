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
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
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
    SourceDescriptorPtr&& sourceDescriptor,
    std::shared_ptr<NES::Runtime::AbstractPoolProvider> bufferPool,
    SourceReturnType::EmitFunction&& emitFunction)
{
    auto schema = sourceDescriptor->getSchema();
    /// Todo #241: Get the new source identfier from the source descriptor and pass it to SourceHandle.

    ///-todo: use general descriptor -> add name to descriptor, make source registry functions static?
    ///-todo: create purely virtual configure function in SourceRegistry
    ///-todo: decide on name: plugin/extension?
    auto sourceRegistry = SourceRegistry();

    ///-todo We need the source name to access the registry
    if (sourceDescriptor->instanceOf<CSVSourceDescriptor>())
    {
        if (sourceRegistry.tryCreate(CSVSource::PLUGIN_NAME).has_value())
        {
            NES_INFO("ConvertLogicalToPhysicalSource: Creating CSV file source");
            auto csvSourceType = sourceDescriptor->as<CSVSourceDescriptor>()->getSourceConfig();
            auto csvSource = sourceRegistry.tryCreateAs<CSVSource>("CSV").value();
            csvSource->configure(*schema, std::move(csvSourceType));

            return std::make_shared<SourceHandle>(
                std::move(originId),
                std::move(schema),
                std::move(bufferPool),
                std::move(emitFunction),
                NUM_SOURCE_LOCAL_BUFFERS,
                std::move(csvSource));
        }
        return nullptr; ///-Todo: error handling
    }
    if (sourceDescriptor->instanceOf<TCPSourceDescriptor>())
    {
        if (sourceRegistry.tryCreate(TCPSource::PLUGIN_NAME).has_value())
        {
            NES_INFO("ConvertLogicalToPhysicalSource: Creating TCP source");
            const auto tcpSourceType = sourceDescriptor->as<TCPSourceDescriptor>()->getSourceConfig();
            auto tcpSource = sourceRegistry.tryCreateAs<TCPSource>("TCP").value();
            tcpSource->configure(*schema, tcpSourceType);

            return std::make_shared<SourceHandle>(
                std::move(originId),
                std::move(schema),
                std::move(bufferPool),
                std::move(emitFunction),
                NUM_SOURCE_LOCAL_BUFFERS,
                std::move(tcpSource));
        }
        return nullptr; /// Todo: error handling
    }
    NES_ERROR("ConvertLogicalToPhysicalSource: Unknown Source Descriptor Type {}", schema->toString());
    throw std::invalid_argument("Unknown Source Descriptor Type");
}

}
