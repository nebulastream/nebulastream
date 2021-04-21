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

#include "../../util/NESTest.hpp"
#include <API/Query.hpp>
#include <Catalogs/LogicalStream.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/OperatorNode.hpp>
#include <Sources/DefaultSource.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>

#include <API/Expressions/Expressions.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Util/VizDumpHandler.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/MemorySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableFilterOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableScanOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableSinkOperator.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableCompleteWindowOperator.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMultiplexOperator.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/Phases/Pipelining/AlwaysBreakPolicy.hpp>
#include <QueryCompiler/Phases/Pipelining/DefaultPipeliningPhase.hpp>
#include <QueryCompiler/Phases/Pipelining/FuseIfPossiblePolicy.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>

using namespace std;
using namespace std;

namespace NES {

using namespace NES::API;
using namespace NES::QueryCompilation;
using namespace NES::QueryCompilation::PhysicalOperators;


class QueryCompilerTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("QueryCompilerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryCompilerTest test class.");
    }

    void SetUp() {}

    void TearDown() { NES_DEBUG("Tear down QueryCompilerTest Test."); }
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
    auto streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->addLogicalStream("streamName", schema);
    auto streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::NodeEngine::create("127.0.0.1", 31337, streamConf, 1, 4096, 1024, 12, 12);
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory);

    auto query = Query::from("streamName").filter(Attribute("F1") == 32).sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);

    auto request = QueryCompilationRequest::Builder(queryPlan, nodeEngine)
                       .dump()
                       .build();
    auto result = queryCompiler->compileQuery(request);

    std::cout << result << std::endl;
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
    auto streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->addLogicalStream("streamName", schema);
    auto streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::NodeEngine::create("127.0.0.1", 31337, streamConf, 1, 4096, 1024, 12, 12);
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory);

    auto query = Query::from("streamName").window(SlidingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10), Seconds(2)))
        .byKey(Attribute("key"))
        .apply(Sum(Attribute("value"))).sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);

    auto request = QueryCompilationRequest::Builder(queryPlan, nodeEngine)
        .dump()
        .build();
    auto result = queryCompiler->compileQuery(request);

    std::cout << result << std::endl;
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
    auto streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->addLogicalStream("streamName", schema);
    auto streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::NodeEngine::create("127.0.0.1", 31337, streamConf, 1, 4096, 1024, 12, 12);
    auto compilerOptions = QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = Phases::DefaultPhaseFactory::create();
    auto queryCompiler = DefaultQueryCompiler::create(compilerOptions, phaseFactory);
    auto query1 = Query::from("streamName");
    auto query2 = Query::from("streamName").filter(Attribute("key") == 32).unionWith(&query1).sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query2.getQueryPlan();

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    queryPlan = typeInferencePhase->execute(queryPlan);

    auto request = QueryCompilationRequest::Builder(queryPlan, nodeEngine)
        .dump()
        .build();
    auto result = queryCompiler->compileQuery(request);

    std::cout << result << std::endl;
}



}// namespace NES