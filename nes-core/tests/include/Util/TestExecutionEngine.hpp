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
#ifndef NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTEXECUTIONENGINE_HPP_
#define NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTEXECUTIONENGINE_HPP_
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <NesBaseTest.hpp>
#include <Network/NetworkChannel.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributedWindowRule.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Services/QueryParsingService.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/DummySink.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestQuery.hpp>
#include <Util/TestQueryCompiler.hpp>
#include <Util/TestSink.hpp>
#include <Util/TestUtils.hpp>
namespace NES {

class TestPhaseProvider : public QueryCompilation::Phases::DefaultPhaseFactory {
  public:
    QueryCompilation::LowerToExecutableQueryPlanPhasePtr
    createLowerToExecutableQueryPlanPhase(QueryCompilation::QueryCompilerOptionsPtr options, bool) override {
        auto sinkProvider = std::make_shared<TestUtils::TestSinkProvider>();
        auto sourceProvider = std::make_shared<TestUtils::TestSourceProvider>(options);
        return QueryCompilation::LowerToExecutableQueryPlanPhase::create(sinkProvider, sourceProvider);
    }
};

class NonRunnableDataSource : public NES::DefaultSource {
  public:
    explicit NonRunnableDataSource(const SchemaPtr& schema,
                                   const Runtime::BufferManagerPtr& bufferManager,
                                   const Runtime::QueryManagerPtr& queryManager,
                                   uint64_t numbersOfBufferToProduce,
                                   uint64_t gatheringInterval,
                                   OperatorId operatorId,
                                   OriginId originId,
                                   size_t numSourceLocalBuffers,
                                   const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors)
        : DefaultSource(schema,
                        bufferManager,
                        queryManager,
                        numbersOfBufferToProduce,
                        gatheringInterval,
                        operatorId,
                        originId,
                        numSourceLocalBuffers,
                        successors) {
        wasGracefullyStopped = NES::Runtime::QueryTerminationType::HardStop;
    }

    void runningRoutine() override {
        open();
        completedPromise.set_value(canTerminate.get_future().get());
        close();
    }

    bool stop(Runtime::QueryTerminationType termination) override {
        canTerminate.set_value(true);
        return NES::DefaultSource::stop(termination);
    }

    Runtime::MemoryLayouts::DynamicTupleBuffer getBuffer() { return allocateBuffer(); }

    void emitBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) {
        auto buf = buffer.getBuffer();
        DataSource::emitWorkFromSource(buf);
    }

  private:
    std::promise<bool> canTerminate;
};

DataSourcePtr createNonRunnableSource(const SchemaPtr& schema,
                                      const Runtime::BufferManagerPtr& bufferManager,
                                      const Runtime::QueryManagerPtr& queryManager,
                                      OperatorId operatorId,
                                      OriginId originId,
                                      size_t numSourceLocalBuffers,
                                      const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<NonRunnableDataSource>(schema,
                                                   bufferManager,
                                                   queryManager,
                                                   /*bufferCnt*/ 1,
                                                   /*frequency*/ 1000,
                                                   operatorId,
                                                   originId,
                                                   numSourceLocalBuffers,
                                                   successors);
}

/**
 * @brief A simple stand alone query execution engine for testing.
 */
class TestExecutionEngine {
  public:
    TestExecutionEngine(QueryCompilation::QueryCompilerOptions::QueryCompiler compiler) {
        auto workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->queryCompiler.queryCompilerType = compiler;
        auto defaultSourceType = DefaultSourceType::create();
        PhysicalSourcePtr sourceConf = PhysicalSource::create("default", "default1", defaultSourceType);
        workerConfiguration->physicalSources.add(sourceConf);
        auto phaseProvider = std::make_shared<TestPhaseProvider>();
        nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                         .setPhaseFactory(phaseProvider)
                         .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                         .build();

        // enable distributed window optimization
        auto optimizerConfiguration = Configurations::OptimizerConfiguration();
        optimizerConfiguration.performDistributedWindowOptimization = true;
        optimizerConfiguration.distributedWindowChildThreshold = 2;
        optimizerConfiguration.distributedWindowCombinerThreshold = 4;
        distributeWindowRule = Optimizer::DistributedWindowRule::create(optimizerConfiguration);
        originIdInferencePhase = Optimizer::OriginIdInferencePhase::create();

        // Initialize the typeInferencePhase with a dummy SourceCatalog & UdfCatalog
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto queryParsingService = QueryParsingService::create(jitCompiler);
        Catalogs::UDF::UdfCatalogPtr udfCatalog = Catalogs::UDF::UdfCatalog::create();
        auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    }

    auto createDateSink(SchemaPtr outputSchema) { return std::make_shared<TestSink>(1, outputSchema, nodeEngine); }

    auto createDataSource(SchemaPtr inputSchema) {
        return std::make_shared<TestUtils::TestSourceDescriptor>(
            inputSchema,
            [&](OperatorId id,
                const SourceDescriptorPtr&,
                const Runtime::NodeEnginePtr& nodeEngine,
                size_t numSourceLocalBuffers,
                std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
                return createNonRunnableSource(inputSchema,
                                               nodeEngine->getBufferManager(),
                                               nodeEngine->getQueryManager(),
                                               id,
                                               0,
                                               numSourceLocalBuffers,
                                               std::move(successors));
            });
    }

    std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> submitQuery(QueryPlanPtr queryPlan) {
        // pre submission optimization
        queryPlan = typeInferencePhase->execute(queryPlan);
        queryPlan = originIdInferencePhase->execute(queryPlan);
        NES_ASSERT(nodeEngine->registerQueryInNodeEngine(queryPlan), "query plan could not be started.");
        NES_ASSERT(nodeEngine->startQuery(queryPlan->getQueryId()), "query plan could not be started.");

        return nodeEngine->getQueryManager()->getQueryExecutionPlan(queryPlan->getQueryId());
    }

    std::shared_ptr<NonRunnableDataSource> getDataSource(std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> plan,
                                                         uint32_t source) {
        NES_ASSERT(!plan->getSources().empty(), "Query plan has no sources ");
        return std::dynamic_pointer_cast<NonRunnableDataSource>(plan->getSources()[source]);
    }

    void emitBuffer(std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> plan, Runtime::TupleBuffer buffer) {
        // todo add support for multiple sources.
        nodeEngine->getQueryManager()->addWorkForNextPipeline(buffer, plan->getPipelines()[0]);
    }

    bool stopQuery(std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> plan) {
        return nodeEngine->stopQuery(plan->getQueryId());
    }

    Runtime::MemoryLayouts::DynamicTupleBuffer getBuffer(const SchemaPtr& schema) {
        auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
        // add support for columnar layout
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
        return Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    }
    bool stop() { return nodeEngine->stop(); }

    Runtime::BufferManagerPtr getBufferManager() const { return nodeEngine->getBufferManager(); }

  private:
    Runtime::NodeEnginePtr nodeEngine;
    Optimizer::DistributeWindowRulePtr distributeWindowRule;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    Optimizer::OriginIdInferencePhasePtr originIdInferencePhase;
};

}// namespace NES

#endif//NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTEXECUTIONENGINE_HPP_
