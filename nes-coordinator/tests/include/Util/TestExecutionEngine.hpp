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
#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTEXECUTIONENGINE_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTEXECUTIONENGINE_HPP_

#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Network/NetworkChannel.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperatorNode.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributedWindowRule.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Services/QueryParsingService.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/DummySink.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/NonRunnableDataSource.hpp>
#include <Util/TestQuery.hpp>
#include <Util/TestQueryCompiler.hpp>
#include <Util/TestSink.hpp>
#include <Util/TestUtils.hpp>

namespace NES::Testing {
class NonRunnableDataSource;
class TestSourceDescriptor;

/**
 * @brief Default values for the test execution engine, otherwise tests will not terminate, as not enough buffers are available
 */
constexpr auto DEFAULT_BUFFERSIZE = 8196;
constexpr auto DEFAULT_NO_BUFFERS_IN_GLOBAL_BM_PER_THREAD = 10240;
constexpr auto DEFAULT_NO_BUFFERS_IN_SOURCE_BM_PER_THREAD = 512;

/**
 * @brief A simple stand alone query execution engine for testing.
 */
class TestExecutionEngine {
  public:
    explicit TestExecutionEngine(
        const QueryCompilation::DumpMode& dumpMode = QueryCompilation::DumpMode::NONE,
        const uint64_t numWorkerThreads = 1,
        const QueryCompilation::StreamJoinStrategy& joinStrategy = QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN,
        const QueryCompilation::WindowingStrategy& windowingStrategy = QueryCompilation::WindowingStrategy::SLICING);

    std::shared_ptr<TestSink> createDataSink(const SchemaPtr& outputSchema, uint32_t expectedTuples = 1);

    template<class Type>
    auto createCollectSink(SchemaPtr outputSchema) {
        return CollectTestSink<Type>::create(outputSchema, nodeEngine);
    }

    std::shared_ptr<SourceDescriptor> createDataSource(SchemaPtr inputSchema);

    std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> submitQuery(QueryPlanPtr queryPlan);

    std::shared_ptr<NonRunnableDataSource> getDataSource(std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> plan,
                                                         uint32_t source);

    void emitBuffer(std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> plan, Runtime::TupleBuffer buffer);

    bool stopQuery(std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> plan,
                   Runtime::QueryTerminationType type = Runtime::QueryTerminationType::HardStop);

    Runtime::MemoryLayouts::DynamicTupleBuffer getBuffer(const SchemaPtr& schema);

    bool stop();

    Runtime::BufferManagerPtr getBufferManager() const;

  private:
    Runtime::NodeEnginePtr nodeEngine;
    Optimizer::DistributeWindowRulePtr distributeWindowRule;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    Optimizer::OriginIdInferencePhasePtr originIdInferencePhase;
};

}// namespace NES::Testing

#endif// NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTEXECUTIONENGINE_HPP_
