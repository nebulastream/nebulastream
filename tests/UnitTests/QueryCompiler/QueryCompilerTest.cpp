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

#include <API/Expressions/Expressions.hpp>
#include <API/QueryAPI.hpp>
#include <Catalogs/LogicalStream.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Nodes/Util/VizDumpHandler.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/MemorySourceDescriptor.hpp>
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
#include <QueryCompiler/IR/Node.hpp>
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
#include <iostream>
#include <memory>

using namespace std;
using namespace std;

namespace NES {

using namespace NES::API;
using namespace NES::QueryCompilation;
using namespace NES::QueryCompilation::PhysicalOperators;

class QueryCompilerTest : public testing::Test {
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
    auto streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    streamCatalog->addLogicalStream("streamName", schema);
    auto streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   31337,
                                                                   streamConf,
                                                                   1,
                                                                   4096,
                                                                   1024,
                                                                   12,
                                                                   12,
                                                                   NES::Runtime::NumaAwarenessFlag::DISABLED);
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

    auto query = Query::from("streamName").filter(Attribute("F1") == 32).sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();

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
    auto streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    streamCatalog->addLogicalStream("streamName", schema);
    auto streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   31337,
                                                                   streamConf,
                                                                   1,
                                                                   4096,
                                                                   1024,
                                                                   12,
                                                                   12,
                                                                   NES::Runtime::NumaAwarenessFlag::DISABLED,
                                                                   "",
                                                                   "Release",
                                                                   "No");
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

    auto query = Query::from("streamName").filter(Attribute("F1") == 32).sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();

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
    auto streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    streamCatalog->addLogicalStream("streamName", schema);
    auto streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   31337,
                                                                   streamConf,
                                                                   1,
                                                                   4096,
                                                                   1024,
                                                                   12,
                                                                   12,
                                                                   NES::Runtime::NumaAwarenessFlag::DISABLED);
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

    auto query = Query::from("streamName")
                     .window(SlidingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10), Seconds(2)))
                     .byKey(Attribute("key"))
                     .apply(Sum(Attribute("value")))
                     .sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();

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
    auto streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    streamCatalog->addLogicalStream("streamName", schema);
    auto streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   31337,
                                                                   streamConf,
                                                                   1,
                                                                   4096,
                                                                   1024,
                                                                   12,
                                                                   12,
                                                                   NES::Runtime::NumaAwarenessFlag::DISABLED);
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

    auto query = Query::from("streamName")
                     .window(SlidingWindow::of(TimeCharacteristic::createEventTime(Attribute("ts")), Seconds(10), Seconds(2)))
                     .byKey(Attribute("key"))
                     .apply(Sum(Attribute("value")))
                     .sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();

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
    auto streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    streamCatalog->addLogicalStream("streamName", schema);
    auto streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   31337,
                                                                   streamConf,
                                                                   1,
                                                                   4096,
                                                                   1024,
                                                                   12,
                                                                   12,
                                                                   NES::Runtime::NumaAwarenessFlag::DISABLED);
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);
    auto query1 = Query::from("streamName");
    auto query2 =
        Query::from("streamName").filter(Attribute("key") == 32).unionWith(query1).sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query2.getQueryPlan();

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
 * |Source| --
 *             \
 * |Source| -- |Join| --- |Sink|
 *
 */
TEST_F(QueryCompilerTest, joinQuery) {
    SchemaPtr schema = Schema::create();
    schema->addField("key", INT32);
    schema->addField("value", INT32);
    auto streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    streamCatalog->addLogicalStream("leftStream", schema);
    streamCatalog->addLogicalStream("rightStream", schema);
    auto streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   31337,
                                                                   streamConf,
                                                                   1,
                                                                   4096,
                                                                   1024,
                                                                   12,
                                                                   12,
                                                                   NES::Runtime::NumaAwarenessFlag::DISABLED);
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);
    auto query1 = Query::from("leftStream");
    auto query2 = Query::from("rightStream")
                      .joinWith(query1)
                      .where(Attribute("leftStream$key"))
                      .equalsTo(Attribute("rightStream$key"))
                      .window(TumblingWindow::of(IngestionTime(), Seconds(10)))
                      .sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query2.getQueryPlan();

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
    schema->addField("F1", INT32);
    auto streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    streamCatalog->addLogicalStream("streamName", schema);
    auto streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   31337,
                                                                   streamConf,
                                                                   1,
                                                                   4096,
                                                                   1024,
                                                                   12,
                                                                   12,
                                                                   NES::Runtime::NumaAwarenessFlag::DISABLED);
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

    auto query = Query::from("streamName").filter(Attribute("F1") == 32).sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);

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