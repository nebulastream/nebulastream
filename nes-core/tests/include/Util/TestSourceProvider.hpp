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
#ifndef NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSOURCEPROVIDER_HPP_
#define NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSOURCEPROVIDER_HPP_

#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
namespace NES::TestUtils {

class TestSourceProvider : public QueryCompilation::DefaultDataSourceProvider {
  public:
    explicit TestSourceProvider(QueryCompilation::QueryCompilerOptionsPtr options);
    DataSourcePtr lower(OperatorId operatorId,
                        OriginId originId,
                        SourceDescriptorPtr sourceDescriptor,
                        Runtime::NodeEnginePtr nodeEngine,
                        std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) override;
};
}// namespace NES::TestUtils
#endif//NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSOURCEPROVIDER_HPP_
