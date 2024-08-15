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
#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/TCPSource.hpp>

namespace NES::QueryCompilation
{

DefaultDataSourceProvider::DefaultDataSourceProvider(QueryCompilerOptionsPtr compilerOptions) : compilerOptions(std::move(compilerOptions))
{
}

DataSourceProviderPtr DefaultDataSourceProvider::create(const QueryCompilerOptionsPtr& compilerOptions)
{
    return std::make_shared<DefaultDataSourceProvider>(compilerOptions);
}

SourceHandlePtr DefaultDataSourceProvider::lower(
    OriginId originId,
    const SourceDescriptorPtr& sourceDescriptor,
    Runtime::BufferManagerPtr bufferManager,
    SourceReturnType::EmitFunction&& emitFunction)
{
    auto schema = sourceDescriptor->getSchema();
    if (sourceDescriptor->instanceOf<CSVSourceDescriptor>())
    {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating CSV file source");
        const auto csvSourceType = sourceDescriptor->as<CSVSourceDescriptor>()->getSourceConfig();

        auto csvSource = std::make_unique<CSVSource>(schema, csvSourceType);
        return std::make_shared<SourceHandle>(
            originId,
            schema,
            bufferManager,
            std::move(emitFunction),
            compilerOptions->getNumSourceLocalBuffers(),
            std::move(csvSource),
            csvSourceType->getNumberOfBuffersToProduce()->getValue());
    }
    if (sourceDescriptor->instanceOf<TCPSourceDescriptor>())
    {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating TCP source");
        const auto tcpSourceType = sourceDescriptor->as<TCPSourceDescriptor>()->getSourceConfig();
        auto tcpSource = std::make_unique<TCPSource>(schema, tcpSourceType);
        return std::make_shared<SourceHandle>(
            originId,
            schema,
            bufferManager,
            std::move(emitFunction),
            compilerOptions->getNumSourceLocalBuffers(),
            std::move(tcpSource),
            0);
    }
    NES_ERROR("ConvertLogicalToPhysicalSource: Unknown Source Descriptor Type {}", schema->toString());
    throw std::invalid_argument("Unknown Source Descriptor Type");
}

} /// namespace NES::QueryCompilation