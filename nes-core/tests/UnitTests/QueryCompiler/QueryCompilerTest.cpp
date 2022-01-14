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
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Nodes/Util/VizDumpHandler.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/MemorySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
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
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <gtest/gtest.h>
#include <NesBaseTest.hpp>
#include <iostream>
#include <memory>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>

using namespace std;
using namespace std;

namespace NES {

using namespace NES::API;
using namespace NES::QueryCompilation;
using namespace NES::QueryCompilation::PhysicalOperators;

class QueryCompilerTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;
    std::shared_ptr<Compiler::JITCompiler> jitCompiler;
    static void SetUpTestCase() {
        NES::setupLogging("QueryCompilerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryCompilerTest test class.");
    }

    void SetUp() override {
        auto cppCompiler = Compiler::CPPCompiler::create();
        jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    void TearDown() override { NES_DEBUG("Tear down QueryCompilerTest Test."); }
};

/**
 * @brief Input Query Plan:
 *
 * |Source| -- |Filter| -- |Sink|
 *
 */
TEST_F(QueryCompilerTest, filterQuery) {
    SchemaPtr schema = Schema::create();
    schema->addField("F1", INT32);
    auto streamCatalog = std::make_shared<SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "logicalSourceName";
    std::string physicalSourceName = "x1";
    streamCatalog->addLogicalStream(logicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto streamConf = PhysicalSource::create(logicalSourceName, physicalSourceName, defaultSourceType);
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   0,
                                                                   {streamConf},
                                                                   1,
                                                                   4096,
                                                                   1024,
                                                                   12,
                                                                   12,
                                                                   Configurations::QueryCompilerConfiguration());
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

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
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
    schema->addField("F1", INT32);
    auto streamCatalog = std::make_shared<SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "logicalSourceName";
    std::string physicalSourceName = "x1";
    streamCatalog->addLogicalStream(logicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto streamConf = PhysicalSource::create(logicalSourceName, physicalSourceName, defaultSourceType);
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   0,
                                                                   {streamConf},
                                                                   1,
                                                                   4096,
                                                                   1024,
                                                                   12,
                                                                   12,
                                                                   Configurations::QueryCompilerConfiguration(),
                                                                   std::weak_ptr<NesWorker>(),
                                                                   NES::Runtime::NumaAwarenessFlag::DISABLED,
                                                                   "");
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

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);

    auto request = QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);

    ASSERT_FALSE(result->hasError());
}
/**
 * @brief Input Query Plan:
 *
 * |Source| -- |window| -- |Sink|
 *
 */
TEST_F(QueryCompilerTest, windowQuery) {
    SchemaPtr schema = Schema::create();
    schema->addField("key", INT32);
    schema->addField("value", INT32);
    auto streamCatalog = std::make_shared<SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "logicalSourceName";
    std::string physicalSourceName = "x1";
    streamCatalog->addLogicalStream(logicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto streamConf = PhysicalSource::create(logicalSourceName, physicalSourceName, defaultSourceType);
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   0,
                                                                   {streamConf},
                                                                   1,
                                                                   4096,
                                                                   1024,
                                                                   12,
                                                                   12,
                                                                   Configurations::QueryCompilerConfiguration());
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

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);

    auto request = QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);
    ASSERT_FALSE(result->hasError());
}

/**
 * @brief Input Query Plan:
 *
 * |Source| -- |window| -- |Sink|
 *
 */
TEST_F(QueryCompilerTest, windowQueryEventTime) {
    SchemaPtr schema = Schema::create();
    schema->addField("key", INT32);
    schema->addField("ts", INT64);
    schema->addField("value", INT32);
    auto streamCatalog = std::make_shared<SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "logicalSourceName";
    std::string physicalSourceName = "x1";
    streamCatalog->addLogicalStream(logicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto streamConf = PhysicalSource::create(logicalSourceName, physicalSourceName, defaultSourceType);
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   0,
                                                                   {streamConf},
                                                                   1,
                                                                   4096,
                                                                   1024,
                                                                   12,
                                                                   12,
                                                                   Configurations::QueryCompilerConfiguration());
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

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);

    auto request = QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);
    ASSERT_FALSE(result->hasError());
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
    schema->addField("key", INT32);
    schema->addField("value", INT32);
    auto streamCatalog = std::make_shared<SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "logicalSourceName";
    std::string physicalSourceName = "x1";
    streamCatalog->addLogicalStream(logicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto streamConf = PhysicalSource::create(logicalSourceName, physicalSourceName, defaultSourceType);
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   0,
                                                                   {streamConf},
                                                                   1,
                                                                   4096,
                                                                   1024,
                                                                   12,
                                                                   12,
                                                                   Configurations::QueryCompilerConfiguration());
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);
    auto query1 = Query::from("logicalSourceName");
    auto query2 = Query::from("logicalSourceName")
                      .filter(Attribute("key") == 32)
                      .unionWith(query1)
                      .sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query2.getQueryPlan();

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
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
    schema->addField("key", INT32);
    schema->addField("value", INT32);
    auto streamCatalog = std::make_shared<SourceCatalog>(queryParsingService);
    const std::string leftSourceLogicalSourceName = "leftSource";
    const std::string rightSourceLogicalSourceName = "rightSource";
    streamCatalog->addLogicalStream(leftSourceLogicalSourceName, schema);
    streamCatalog->addLogicalStream(rightSourceLogicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto streamConf1 = PhysicalSource::create(leftSourceLogicalSourceName, "x1", defaultSourceType);
    auto streamConf2 = PhysicalSource::create(rightSourceLogicalSourceName, "x1", defaultSourceType);
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   0,
                                                                   {streamConf1, streamConf2},
                                                                   1,
                                                                   4096,
                                                                   1024,
                                                                   12,
                                                                   12,
                                                                   Configurations::QueryCompilerConfiguration());
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

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);

    auto request = QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);
    ASSERT_FALSE(result->hasError());
}

class CustomPipelineStageOne : public Runtime::Execution::ExecutablePipelineStage {
  public:
    ExecutionResult
    execute(Runtime::TupleBuffer& buffer, Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext, Runtime::WorkerContext&) override {
        for(int i = 0 ; i < buffer.getNumberOfTuples(); i++){
            buffer.getBuffer()
        }
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
    schema->addField("F1", INT32);
    auto streamCatalog = std::make_shared<SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "logicalSourceName";
    std::string physicalSourceName = "x1";
    streamCatalog->addLogicalStream(logicalSourceName, schema);
    auto defaultSourceType = DefaultSourceType::create();
    auto streamConf = PhysicalSource::create(logicalSourceName, physicalSourceName, defaultSourceType);
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   0,
                                                                   {streamConf},
                                                                   1,
                                                                   4096,
                                                                   1024,
                                                                   12,
                                                                   12,
                                                                   Configurations::QueryCompilerConfiguration());
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

    auto query = Query::from("logicalSourceName").filter(Attribute("F1") == 32).sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
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
}

}// namespace NES