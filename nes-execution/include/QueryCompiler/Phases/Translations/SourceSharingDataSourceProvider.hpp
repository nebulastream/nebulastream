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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_SOURCESHARINGDATASOURCEPROVIDER_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_SOURCESHARINGDATASOURCEPROVIDER_HPP_

#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES {
namespace QueryCompilation {

/**
 * @brief Provider to transform a source descriptor to executable DataSource.
 */
class SourceSharingDataSourceProvider : public DefaultDataSourceProvider {
  public:
    explicit SourceSharingDataSourceProvider(QueryCompilerOptionsPtr compilerOptions);
    static DataSourceProviderPtr create(const QueryCompilerOptionsPtr& compilerOptions);
    /**
     * @brief Lowers a source descriptor to a executable data source.
     * @param sourceId id of the data source
     * @param sourceDescriptor
     * @param nodeEngine
     * @param successors
     * @return DataSourcePtr
     */
    virtual DataSourcePtr lower(OperatorId operatorId,
                                OriginId originId,
                                SourceDescriptorPtr sourceDescriptor,
                                Runtime::NodeEnginePtr nodeEngine,
                                std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);

    virtual ~SourceSharingDataSourceProvider() = default;

  protected:
    std::map<std::pair<std::string, std::string>, DataSourcePtr> sourceDescriptorToDataSourceMap;
};
}// namespace QueryCompilation
}// namespace NES

#endif // NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_SOURCESHARINGDATASOURCEPROVIDER_HPP_
