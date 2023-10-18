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

#include <API/Expressions/Expressions.hpp>
#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/MemorySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributedWindowRule.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalExternalOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/Pipelining/DefaultPipeliningPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/ExecutableQueryPlanStatus.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/ReconfigurationMessage.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Operators/LogicalOperators/Windows/DistributionCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Actions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Types/SlidingWindow.hpp>
#include <Operators/LogicalOperators/Windows/Types/TumblingWindow.hpp>
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>

using namespace std;
using namespace std;

namespace NES {

using namespace NES::API;
using namespace NES::QueryCompilation;
using namespace NES::QueryCompilation::PhysicalOperators;

class QueryCompilerTest : public Testing::BaseUnitTest {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;
    std::shared_ptr<Compiler::JITCompiler> jitCompiler;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryCompilerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryCompilerTest test class.");
    }

    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        auto cppCompiler = Compiler::CPPCompiler::create();
        jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
    }

    void cleanUpPlan(Runtime::Execution::ExecutableQueryPlanPtr plan) {
        if (plan->getStatus() != Runtime::Execution::ExecutableQueryPlanStatus::Running) {
            return;
        }
        std::for_each(plan->getSources().begin(), plan->getSources().end(), [plan](auto source) {
            plan->notifySourceCompletion(source, Runtime::QueryTerminationType::Graceful);
        });
        std::for_each(plan->getPipelines().begin(), plan->getPipelines().end(), [plan](auto pipeline) {
            plan->notifyPipelineCompletion(pipeline, Runtime::QueryTerminationType::Graceful);
        });
        std::for_each(plan->getSinks().begin(), plan->getSinks().end(), [plan](auto sink) {
            plan->notifySinkCompletion(sink, Runtime::QueryTerminationType::Graceful);
        });

        auto task = Runtime::ReconfigurationMessage(plan->getQueryId(),
                                                    plan->getQuerySubPlanId(),
                                                    NES::Runtime::ReconfigurationType::SoftEndOfStream,
                                                    plan);
        plan->postReconfigurationCallback(task);

        ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Finished);
    }
};

/**
 * @brief Input Query Plan:
 *
 * |Source| -- |Filter| -- |Sink|
 *
 */
TEST_F(QueryCompilerTest, filterQuery) {
    SchemaPtr schema = Schema::create();
    schema->addField("F1", BasicType::INT32);
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "logicalSourceName";
    std::string physicalSourceName = "x1";
    sourceCatalog->addLogicalSource(logicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto sourceConf = PhysicalSource::create(logicalSourceName, physicalSourceName, defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(sourceConf);
    workerConfiguration->numberOfBuffersInSourceLocalBufferPool.setValue(12);
    workerConfiguration->numberOfBuffersPerWorker.setValue(12);

    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                          .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                          .build();
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

    auto query = Query::from(logicalSourceName).filter(Attribute("F1") == 32).sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();
    vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

    EXPECT_TRUE(!sourceOperators.empty());
    EXPECT_EQ(sourceOperators.size(), 1u);
    auto sourceDescriptor = sourceOperators[0]->getSourceDescriptor();
    sourceDescriptor->setPhysicalSourceName(physicalSourceName);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);

    auto request = QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);

    ASSERT_FALSE(result->hasError());
    cleanUpPlan(result->getExecutableQueryPlan());
}

#ifdef TFDEF
/**
 * @brief Input Query Plan:
 *
 * |Source| -- |inferModel| -- |Sink|
 *
 */
TEST_F(QueryCompilerTest, inferModelQuery) {
    SchemaPtr schema = Schema::create();
    schema->addField("F1", BasicType::INT32);
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "logicalSourceName";
    std::string physicalSourceName = "x1";
    sourceCatalog->addLogicalSource(logicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto sourceConf = PhysicalSource::create(logicalSourceName, physicalSourceName, defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(sourceConf);
    workerConfiguration->numberOfBuffersInSourceLocalBufferPool.setValue(12);
    workerConfiguration->numberOfBuffersPerWorker.setValue(12);

    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                          .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                          .build();
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

    std::filesystem::copy(std::filesystem::path(TEST_DATA_DIRECTORY) / "iris_95acc.tflite",
                          "/tmp/iris_95acc.tflite",
                          std::filesystem::copy_options::overwrite_existing);

    auto query = Query::from(logicalSourceName)
                     .inferModel(std::filesystem::path(TEST_DATA_DIRECTORY) / "iris_95acc.tflite",
                                 {Attribute("F1"), Attribute("F1"), Attribute("F1"), Attribute("F1")},
                                 {Attribute("iris0", BasicType::FLOAT32),
                                  Attribute("iris1", BasicType::FLOAT32),
                                  Attribute("iris2", BasicType::FLOAT32)})
                     .sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();
    vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

    EXPECT_TRUE(!sourceOperators.empty());
    EXPECT_EQ(sourceOperators.size(), 1u);
    auto sourceDescriptor = sourceOperators[0]->getSourceDescriptor();
    sourceDescriptor->setPhysicalSourceName(physicalSourceName);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);

    auto request = QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);

    ASSERT_FALSE(result->hasError());
}
#endif

/**
 * @brief Input Query Plan:
 *
 * |Source| -- |Map| -- |Sink|
 *
 */
TEST_F(QueryCompilerTest, mapQuery) {
    SchemaPtr schema = Schema::create();
    schema->addField("F1", BasicType::INT32);
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "logicalSourceName";
    std::string physicalSourceName = "x1";
    sourceCatalog->addLogicalSource(logicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto sourceConf = PhysicalSource::create(logicalSourceName, physicalSourceName, defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(sourceConf);
    workerConfiguration->numberOfBuffersInSourceLocalBufferPool.setValue(12);
    workerConfiguration->numberOfBuffersPerWorker.setValue(12);

    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                          .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                          .build();
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

    auto query =
        Query::from(logicalSourceName).map(Attribute("F1") = Attribute("F1") + 2.0).sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();
    vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

    EXPECT_TRUE(!sourceOperators.empty());
    EXPECT_EQ(sourceOperators.size(), 1u);
    auto sourceDescriptor = sourceOperators[0]->getSourceDescriptor();
    sourceDescriptor->setPhysicalSourceName(physicalSourceName);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);

    auto request = QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);

    ASSERT_FALSE(result->hasError());
}

/**
 * @brief Input Query Plan:
 *
 * |Source| -- |Filter| -- |Sink|
 *
 */
TEST_F(QueryCompilerTest, filterQueryBitmask) {
    SchemaPtr schema = Schema::create();
    schema->addField("F1", BasicType::INT32);
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "logicalSourceName";
    std::string physicalSourceName = "x1";
    sourceCatalog->addLogicalSource(logicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto sourceConf = PhysicalSource::create(logicalSourceName, physicalSourceName, defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(sourceConf);
    workerConfiguration->numberOfBuffersInSourceLocalBufferPool.setValue(12);
    workerConfiguration->numberOfBuffersPerWorker.setValue(12);

    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                          .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                          .build();
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

    auto query = Query::from("logicalSourceName").filter(Attribute("F1") == 32).sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();
    vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    EXPECT_TRUE(!sourceOperators.empty());
    EXPECT_EQ(sourceOperators.size(), 1u);
    auto sourceDescriptor = sourceOperators[0]->getSourceDescriptor();
    sourceDescriptor->setPhysicalSourceName(physicalSourceName);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);

    auto request = QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);

    ASSERT_FALSE(result->hasError());
    cleanUpPlan(result->getExecutableQueryPlan());
}
/**
 * @brief Input Query Plan:
 *
 * |Source| -- |window| -- |Sink|
 *
 */
TEST_F(QueryCompilerTest, windowQuery) {
    SchemaPtr schema = Schema::create();
    schema->addField("key", BasicType::INT32);
    schema->addField("value", BasicType::INT32);
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "logicalSourceName";
    std::string physicalSourceName = "x1";
    sourceCatalog->addLogicalSource(logicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto sourceConf = PhysicalSource::create(logicalSourceName, physicalSourceName, defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(sourceConf);
    workerConfiguration->numberOfBuffersInSourceLocalBufferPool.setValue(12);
    workerConfiguration->numberOfBuffersPerWorker.setValue(12);

    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                          .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                          .build();
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

    auto query = Query::from("logicalSourceName")
                     .window(SlidingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10), Seconds(2)))
                     .byKey(Attribute("key"))
                     .apply(Sum(Attribute("value")))
                     .sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();
    vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    EXPECT_TRUE(!sourceOperators.empty());
    EXPECT_EQ(sourceOperators.size(), 1u);
    auto sourceDescriptor = sourceOperators[0]->getSourceDescriptor();
    sourceDescriptor->setPhysicalSourceName(physicalSourceName);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);
    auto inferOriginPhase = Optimizer::OriginIdInferencePhase::create();
    queryPlan = inferOriginPhase->execute(queryPlan);
    auto request = QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);
    ASSERT_FALSE(result->hasError());
    cleanUpPlan(result->getExecutableQueryPlan());
}

/**
 * @brief Input Query Plan:
 *
 * |Source| -- |window| -- |Sink|
 *
 */
TEST_F(QueryCompilerTest, windowQueryEventTime) {
    SchemaPtr schema = Schema::create();
    schema->addField("key", BasicType::INT32);
    schema->addField("ts", BasicType::INT64);
    schema->addField("value", BasicType::INT32);
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "logicalSourceName";
    std::string physicalSourceName = "x1";
    sourceCatalog->addLogicalSource(logicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto sourceConf = PhysicalSource::create(logicalSourceName, physicalSourceName, defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(sourceConf);
    workerConfiguration->numberOfBuffersInSourceLocalBufferPool.setValue(12);
    workerConfiguration->numberOfBuffersPerWorker.setValue(12);

    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                          .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                          .build();
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

    auto query = Query::from("logicalSourceName")
                     .window(SlidingWindow::of(TimeCharacteristic::createEventTime(Attribute("ts")), Seconds(10), Seconds(2)))
                     .byKey(Attribute("key"))
                     .apply(Sum(Attribute("value")))
                     .sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();
    vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    EXPECT_TRUE(!sourceOperators.empty());
    EXPECT_EQ(sourceOperators.size(), 1u);
    auto sourceDescriptor = sourceOperators[0]->getSourceDescriptor();
    sourceDescriptor->setPhysicalSourceName(physicalSourceName);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);
    auto inferOriginPhase = Optimizer::OriginIdInferencePhase::create();
    queryPlan = inferOriginPhase->execute(queryPlan);

    auto request = QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);
    ASSERT_FALSE(result->hasError());
    cleanUpPlan(result->getExecutableQueryPlan());
}

/**
 * @brief Input Query Plan:
 *
 * |Source| --          --
 *                          \
 * |Source| -- |Filter| -- |Union| --- |Sink|
 *
 */
TEST_F(QueryCompilerTest, unionQuery) {
    SchemaPtr schema = Schema::create();
    schema->addField("key", BasicType::INT32);
    schema->addField("value", BasicType::INT32);
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "logicalSourceName";
    std::string physicalSourceName = "x1";
    sourceCatalog->addLogicalSource(logicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto sourceConf = PhysicalSource::create(logicalSourceName, physicalSourceName, defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(sourceConf);
    workerConfiguration->numberOfBuffersInSourceLocalBufferPool.setValue(12);
    workerConfiguration->numberOfBuffersPerWorker.setValue(12);

    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                          .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                          .build();
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);
    auto query1 = Query::from("logicalSourceName");
    auto query2 = Query::from("logicalSourceName")
                      .filter(Attribute("key") == 32)
                      .unionWith(query1)
                      .sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query2.getQueryPlan();

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);
    vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    EXPECT_TRUE(!sourceOperators.empty());
    EXPECT_EQ(sourceOperators.size(), 2u);
    auto sourceDescriptor1 = sourceOperators[0]->getSourceDescriptor();
    sourceDescriptor1->setPhysicalSourceName(physicalSourceName);
    auto sourceDescriptor2 = sourceOperators[1]->getSourceDescriptor();
    sourceDescriptor2->setPhysicalSourceName(physicalSourceName);

    auto request = QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);
    ASSERT_FALSE(result->hasError());
    cleanUpPlan(result->getExecutableQueryPlan());
}

/**
 * @brief Input Query Plan:
 *
 * |Source| --
 *             \
 * |Source| -- |Join| --- |Sink|
 *
 */
TEST_F(QueryCompilerTest, joinQuery) {
    SchemaPtr schema = Schema::create();
    schema->addField("key", BasicType::INT32);
    schema->addField("value", BasicType::INT32);
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    const std::string leftSourceLogicalSourceName = "leftSource";
    const std::string rightSourceLogicalSourceName = "rightSource";
    sourceCatalog->addLogicalSource(leftSourceLogicalSourceName, schema);
    sourceCatalog->addLogicalSource(rightSourceLogicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto sourceConf1 = PhysicalSource::create(leftSourceLogicalSourceName, "x1", defaultSourceType);
    auto sourceConf2 = PhysicalSource::create(rightSourceLogicalSourceName, "x1", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(sourceConf1);
    workerConfiguration->physicalSourceTypes.add(sourceConf2);
    workerConfiguration->numberOfBuffersInSourceLocalBufferPool.setValue(12);
    workerConfiguration->numberOfBuffersPerWorker.setValue(12);

    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                          .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                          .build();
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);
    auto query1 = Query::from(leftSourceLogicalSourceName);
    auto query2 = Query::from(rightSourceLogicalSourceName)
                      .joinWith(query1)
                      .where(Attribute("leftSource$key"))
                      .equalsTo(Attribute("rightSource$key"))
                      .window(TumblingWindow::of(IngestionTime(), Seconds(10)))
                      .sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query2.getQueryPlan();
    vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    EXPECT_TRUE(!sourceOperators.empty());
    EXPECT_EQ(sourceOperators.size(), 2u);
    auto sourceDescriptor1 = sourceOperators[0]->getSourceDescriptor();
    sourceDescriptor1->setPhysicalSourceName("x1");
    auto sourceDescriptor2 = sourceOperators[1]->getSourceDescriptor();
    sourceDescriptor2->setPhysicalSourceName("x1");

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);

    auto request = QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);
    ASSERT_FALSE(result->hasError());
    cleanUpPlan(result->getExecutableQueryPlan());
}

class CustomPipelineStageOne : public Runtime::Execution::ExecutablePipelineStage {
  public:
    ExecutionResult
    execute(Runtime::TupleBuffer&, Runtime::Execution::PipelineExecutionContext&, Runtime::WorkerContext&) override {
        return ExecutionResult::Ok;
    }
};

/**
 * @brief Input Query Plan:
 *
 * |Source| -- |Filter| -- |ExternalOperator| -- |Sink|
 *
 */
TEST_F(QueryCompilerTest, externalOperatorTest) {
    SchemaPtr schema = Schema::create();
    schema->addField("F1", BasicType::INT32);
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "logicalSourceName";
    std::string physicalSourceName = "x1";
    sourceCatalog->addLogicalSource(logicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto sourceConf = PhysicalSource::create(logicalSourceName, physicalSourceName, defaultSourceType);

    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(sourceConf);
    workerConfiguration->numberOfBuffersInSourceLocalBufferPool.setValue(12);
    workerConfiguration->numberOfBuffersPerWorker.setValue(12);

    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                          .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                          .build();
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

    auto query = Query::from("logicalSourceName").filter(Attribute("F1") == 32).sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);
    vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    EXPECT_TRUE(!sourceOperators.empty());
    EXPECT_EQ(sourceOperators.size(), 1u);
    auto sourceDescriptor = sourceOperators[0]->getSourceDescriptor();
    sourceDescriptor->setPhysicalSourceName(physicalSourceName);

    // add physical operator behind the filter
    auto filterOperator = queryPlan->getOperatorByType<FilterLogicalOperatorNode>()[0];

    auto customPipelineStage = std::make_shared<CustomPipelineStageOne>();
    auto externalOperator = PhysicalExternalOperator::create(SchemaPtr(), SchemaPtr(), customPipelineStage);

    filterOperator->insertBetweenThisAndParentNodes(externalOperator);
    auto request = QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);

    ASSERT_FALSE(result->hasError());
    cleanUpPlan(result->getExecutableQueryPlan());
}

}// namespace NES