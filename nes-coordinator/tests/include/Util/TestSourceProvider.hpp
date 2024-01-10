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
#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTSOURCEPROVIDER_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTSOURCEPROVIDER_HPP_

#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
namespace NES::TestUtils {

class TestSourceProvider : public QueryCompilation::DefaultDataSourceProvider {
  public:
    /**
     * @brief Constructor of a TestSourceProvider
     * @param options
     */
    explicit TestSourceProvider(QueryCompilation::QueryCompilerOptionsPtr options);

    /**
     * @brief Lowers a source descriptor to a executable data source.
     * @param operatorId id of the data source
     * @param originId
     * @param sourceDescriptor
     * @param nodeEngine
     * @param successors
     * @return DataSourcePtr
     */
    DataSourcePtr lower(OperatorId operatorId,
                        OriginId originId,
                        SourceDescriptorPtr sourceDescriptor,
                        Runtime::NodeEnginePtr nodeEngine,
                        std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) override;
};
}// namespace NES::TestUtils
#endif // NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTSOURCEPROVIDER_HPP_
