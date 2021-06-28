/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DATASOURCEPROVIDER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DATASOURCEPROVIDER_HPP_
#include <Operators/OperatorId.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
namespace NES {
namespace QueryCompilation {

/**
 * @brief Provider to transform a source descriptor to executable DataSource.
 */
class DataSourceProvider {
  public:
    explicit DataSourceProvider(QueryCompilerOptionsPtr compilerOptions);
    static DataSourceProviderPtr create(const QueryCompilerOptionsPtr& compilerOptions);
    /**
     * @brief Lowers a source descriptor to a executable data source.
     * @param operatorId id of the data source
     * @param logicalOperatorId id of the operator in logical query plan this source is created from
     * @param sourceDescriptor
     * @param nodeEngine
     * @param successors
     * @return DataSourcePtr
     */
    virtual DataSourcePtr lower(OperatorId operatorId,
                                OperatorId logicalOperatorId,
                                SourceDescriptorPtr sourceDescriptor,
                                Runtime::NodeEnginePtr nodeEngine,
                                std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);

    virtual ~DataSourceProvider() = default;

  protected:
    QueryCompilerOptionsPtr compilerOptions;
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DATASOURCEPROVIDER_HPP_
