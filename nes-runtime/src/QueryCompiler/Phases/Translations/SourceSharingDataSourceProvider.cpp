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
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <QueryCompiler/Phases/Translations/ConvertLogicalToPhysicalSource.hpp>
#include <QueryCompiler/Phases/Translations/SourceSharingDataSourceProvider.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <utility>

namespace NES::QueryCompilation {

SourceSharingDataSourceProvider::SourceSharingDataSourceProvider(QueryCompilerOptionsPtr compilerOptions)
    : DefaultDataSourceProvider(std::move(compilerOptions)) {}

DataSourceProviderPtr QueryCompilation::SourceSharingDataSourceProvider::create(const QueryCompilerOptionsPtr& compilerOptions) {
    return std::make_shared<SourceSharingDataSourceProvider>(compilerOptions);
}

DataSourcePtr SourceSharingDataSourceProvider::lower(OperatorId operatorId,
                                                     OriginId originId,
                                                     SourceDescriptorPtr sourceDescriptor,
                                                     Runtime::NodeEnginePtr nodeEngine,
                                                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) {
    if (sourceDescriptor->instanceOf<Network::NetworkSourceDescriptor>()) {
        return ConvertLogicalToPhysicalSource::createDataSource(operatorId,
                                                                originId,
                                                                std::move(sourceDescriptor),
                                                                std::move(nodeEngine),
                                                                compilerOptions->getNumSourceLocalBuffers(),
                                                                std::move(successors));
    }
    NES_ASSERT(sourceDescriptor->getLogicalSourceName() != "", "The source name is not allowed to be null");

    auto searchEntry = std::make_pair(sourceDescriptor->getLogicalSourceName(), sourceDescriptor->getPhysicalSourceName());
    if (sourceDescriptorToDataSourceMap.contains(searchEntry)) {
        NES_DEBUG("using already existing source for source sharing for logical name {}",
                  sourceDescriptor->getLogicalSourceName());
        sourceDescriptorToDataSourceMap[searchEntry]->addExecutableSuccessors(successors);
        sourceDescriptorToDataSourceMap[searchEntry]->incrementNumberOfConsumerQueries();
        return sourceDescriptorToDataSourceMap[searchEntry];
    } else {
        NES_DEBUG("Create first source for source sharing for logical name  {}", sourceDescriptor->getLogicalSourceName());
        auto source = ConvertLogicalToPhysicalSource::createDataSource(operatorId,
                                                                       originId,
                                                                       std::move(sourceDescriptor),
                                                                       std::move(nodeEngine),
                                                                       compilerOptions->getNumSourceLocalBuffers(),
                                                                       std::move(successors));
        sourceDescriptorToDataSourceMap[searchEntry] = source;
        source->setSourceSharing(true);
        return source;
    }
}

}// namespace NES::QueryCompilation