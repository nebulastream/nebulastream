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
#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTSINKPROVIDER_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTSINKPROVIDER_HPP_

#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <QueryCompiler/NautilusQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/TestPhaseProvider.hpp>

namespace NES::TestUtils {

class TestSinkProvider : public QueryCompilation::DataSinkProvider {
  public:
    /**
     * @brief Lowers a sink descriptor to a executable data sink.
     * @param operatorId id of this sink
     * @param sinkDescriptor the sink descriptor
     * @param schema the schema of the sink
     * @param nodeEngine the node engine
     * @param querySubPlanId
     * @param numOfProducers
     * @return DataSinkPtr
     */
    DataSinkPtr lower(OperatorId sinkId,
                      SinkDescriptorPtr sinkDescriptor,
                      SchemaPtr schema,
                      Runtime::NodeEnginePtr nodeEngine,
                      const QueryCompilation::PipelineQueryPlanPtr& querySubPlan,
                      size_t numOfProducers,
                      uint16_t numberOfInputSources) override;
};
}// namespace NES::TestUtils
#endif  // NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTSINKPROVIDER_HPP_
