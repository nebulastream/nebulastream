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

#include <Util/NonRunnableDataSource.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSourceDescriptor.hpp>

namespace NES::Testing {

TestExecutionEngine::TestExecutionEngine(const QueryCompilation::QueryCompilerOptions::QueryCompiler& compiler,
                                         const QueryCompilation::QueryCompilerOptions::DumpMode& dumpMode,
                                         const uint64_t numWorkerThreads,
                                         const QueryCompilation::StreamJoinStrategy& joinStrategy,
                                         const QueryCompilation::WindowingStrategy& windowingStrategy) {
    auto workerConfiguration = WorkerConfiguration::create();

    workerConfiguration->queryCompiler.joinStrategy = joinStrategy;
    workerConfiguration->queryCompiler.queryCompilerType = compiler;
    workerConfiguration->queryCompiler.nautilusBackend = QueryCompilation::QueryCompilerOptions::NautilusBackend::MLIR_COMPILER;
    workerConfiguration->queryCompiler.queryCompilerDumpMode = dumpMode;
    workerConfiguration->queryCompiler.windowingStrategy = windowingStrategy;
    workerConfiguration->queryCompiler.compilationStrategy = QueryCompilation::QueryCompilerOptions::CompilationStrategy::DEBUG;
    workerConfiguration->numWorkerThreads = numWorkerThreads;
    workerConfiguration->bufferSizeInBytes = 8196;
    workerConfiguration->numberOfBuffersInGlobalBufferManager = numWorkerThreads * 10240;
    workerConfiguration->numberOfBuffersInSourceLocalBufferPool = numWorkerThreads * 512;

    auto defaultSourceType = DefaultSourceType::create();
    PhysicalSourcePtr sourceConf = PhysicalSource::create("default", "default1", defaultSourceType);
    workerConfiguration->physicalSources.add(sourceConf);
    auto phaseProvider = std::make_shared<TestUtils::TestPhaseProvider>();
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

    // Initialize the typeInferencePhase with a dummy SourceCatalog & UDFCatalog
    Catalogs::UDF::UDFCatalogPtr udfCatalog = Catalogs::UDF::UDFCatalog::create();
    // We inject an invalid query parsing service as it is not used in the tests.
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
}

std::shared_ptr<TestSink> TestExecutionEngine::createDataSink(const SchemaPtr& outputSchema, uint32_t expectedBuffer) {
    return std::make_shared<TestSink>(expectedBuffer, outputSchema, nodeEngine);
}

std::shared_ptr<SourceDescriptor> TestExecutionEngine::createDataSource(SchemaPtr inputSchema) {
    return std::make_shared<TestUtils::TestSourceDescriptor>(
        inputSchema,
        // We require inputSchema as a lambda function arg since capturing it can lead to using a corrupted schema.
        [&](SchemaPtr inputSchema,
            OperatorId id,
            OriginId originId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr& nodeEngine,
            size_t numSourceLocalBuffers,
            const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) -> DataSourcePtr {
            return createNonRunnableSource(inputSchema,
                                           nodeEngine->getBufferManager(),
                                           nodeEngine->getQueryManager(),
                                           id,
                                           originId,
                                           numSourceLocalBuffers,
                                           successors,
                                           Runtime::QueryTerminationType::Graceful);
        });
}

std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> TestExecutionEngine::submitQuery(QueryPlanPtr queryPlan) {
    // pre submission optimization
    queryPlan = typeInferencePhase->execute(queryPlan);
    queryPlan = originIdInferencePhase->execute(queryPlan);
    NES_ASSERT(nodeEngine->registerQueryInNodeEngine(queryPlan), "query plan could not be started.");
    NES_ASSERT(nodeEngine->startQuery(queryPlan->getQueryId()), "query plan could not be started.");

    return nodeEngine->getQueryManager()->getQueryExecutionPlan(queryPlan->getQueryId());
}

std::shared_ptr<NonRunnableDataSource>
TestExecutionEngine::getDataSource(std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> plan, uint32_t source) {
    NES_ASSERT(!plan->getSources().empty(), "Query plan has no sources ");
    return std::dynamic_pointer_cast<NonRunnableDataSource>(plan->getSources()[source]);
}

void TestExecutionEngine::emitBuffer(std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> plan, Runtime::TupleBuffer buffer) {
    // todo add support for multiple sources.
    nodeEngine->getQueryManager()->addWorkForNextPipeline(buffer, plan->getPipelines()[0]);
}

bool TestExecutionEngine::stopQuery(std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> plan,
                                    Runtime::QueryTerminationType type) {
    return nodeEngine->getQueryManager()->stopQuery(plan, type);
}

Runtime::MemoryLayouts::DynamicTupleBuffer TestExecutionEngine::getBuffer(const SchemaPtr& schema) {
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    // add support for columnar layout
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
    return Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
}

bool TestExecutionEngine::stop() { return nodeEngine->stop(); }

Runtime::BufferManagerPtr TestExecutionEngine::getBufferManager() const { return nodeEngine->getBufferManager(); }

}// namespace NES::Testing