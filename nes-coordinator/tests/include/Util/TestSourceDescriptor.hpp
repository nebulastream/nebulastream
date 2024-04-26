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
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <QueryCompiler/NautilusQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/TestPhaseProvider.hpp>

#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTSOURCEDESCRIPTOR_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTSOURCEDESCRIPTOR_HPP_

namespace NES::TestUtils {

class TestSourceDescriptor : public SourceDescriptor {
  public:
    /**
     * @brief Constructor for a TestSourceDescriptor
     * @param schema
     * @param createSourceFunction
     */
    TestSourceDescriptor(
        SchemaPtr schema,
        std::function<DataSourcePtr(SchemaPtr schema,
                                    OperatorId,
                                    OriginId,
                                    StatisticId,
                                    SourceDescriptorPtr,
                                    Runtime::NodeEnginePtr,
                                    size_t,
                                    std::vector<Runtime::Execution::SuccessorExecutablePipeline>)> createSourceFunction);
    /**
     * @brief Factory method for creating a DataSource
     * @param operatorId
     * @param originId
     * @param sourceDescriptor
     * @param nodeEngine
     * @param numSourceLocalBuffers
     * @param successors
     * @return DataSource
     */
    DataSourcePtr create(OperatorId operatorId,
                         OriginId originId,
                         StatisticId statisticId,
                         SourceDescriptorPtr sourceDescriptor,
                         Runtime::NodeEnginePtr nodeEngine,
                         size_t numSourceLocalBuffers,
                         std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);

    /**
     * @brief Creates a string representation
     * @return String representation
     */
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Checks if two SourceDescriptors are equal
     * @return True, if equal otherwise false
     */
    [[nodiscard]] bool equal(SourceDescriptorPtr const&) const override;

    /**
     * @brief Copies this SourceDescriptors
     * @return New SourceDescriptor
     */
    SourceDescriptorPtr copy() override;

  private:
    std::function<DataSourcePtr(SchemaPtr schema,
                                OperatorId,
                                OriginId,
                                StatisticId,
                                SourceDescriptorPtr,
                                Runtime::NodeEnginePtr,
                                size_t,
                                std::vector<Runtime::Execution::SuccessorExecutablePipeline>)>
        createSourceFunction;
};

}// namespace NES::TestUtils

#endif // NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTSOURCEDESCRIPTOR_HPP_
