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
#include "gtest/gtest.h"

#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayout.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayoutField.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>
#include <utility>

#include "../../util/DummySink.hpp"
#include "../../util/SchemaSourceDescriptor.hpp"
#include "../../util/TestQuery.hpp"
#include "../../util/TestQueryCompiler.hpp"
#include "../../util/TestSink.hpp"
#include <Catalogs/StreamCatalog.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalExternalOperator.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Topology/TopologyNode.hpp>

#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>

#include <Windowing/MVContinuousWindow.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Util/libcuckoo/cuckoohash_map.hh>

#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Views/AIMBenchmark.hpp>

using namespace NES;
using Runtime::TupleBuffer;

uint64_t rpcPort = 4000;
uint64_t restPort = 8081;

class MaterializedViewLogTest : public testing::Test {
public:
    static void SetUpTestCase() {
        NES::setupLogging("MaterializedViewLogTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MaterializedViewLogTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        nodeEngine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);
        testSchema = Schema::create()
                ->addField("test$id", BasicType::INT64)
                ->addField("test$one", BasicType::INT64)
                ->addField("test$value", BasicType::INT64);
        csvSchema = Schema::create()
                ->addField("mv$id", BasicType::INT32)
                ->addField("mv$cost", BasicType::FLOAT32)
                ->addField("mv$duration", BasicType::INT32)
                ->addField("mv$timestamp", BasicType::INT64)
                ->addField("mv$isLongDistance", BasicType::BOOLEAN);
    }
    SchemaPtr testSchema;
    SchemaPtr csvSchema;
    Runtime::NodeEnginePtr nodeEngine;
};

/*uint64_t startAdhocQuery1(//std::shared_ptr<TestSink> testSink,
                      Runtime::NodeEnginePtr nodeEngine,
                      SchemaPtr testSchema, // or csvSchema
                      uint64_t queryId,
                      std::shared_ptr<MaterializedView<uint64_t,EventRecord>> mv) {
    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
            testSchema,
            [&](OperatorId id,
                    const SourceDescriptorPtr&,
                    const Runtime::NodeEnginePtr&,
                    size_t numSourceLocalBuffers,
                    std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
                return createDefaultDataSourceWithSchemaForOneBuffer(testSchema,
                                                                     nodeEngine->getBufferManager(),
                                                                     nodeEngine->getQueryManager(), id,
                                                                     numSourceLocalBuffers,
                                                                     std::move(successors));});

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    auto sinkOperator = queryPlan->getOperatorByType<LogicalUnaryOperatorNode>()[1];

    auto customPipelineStage = std::make_shared<AdHocQuery1>(mv);
    auto externalOperator =
            NES::QueryCompilation::PhysicalOperators::PhysicalExternalOperator::create(SchemaPtr(), SchemaPtr(), customPipelineStage);

    sinkOperator->insertBetweenThisAndParentNodes(externalOperator);

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();

    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    plan->setup();

    auto start = std::chrono::high_resolution_clock::now();
    plan->start(nodeEngine->getStateManager());
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 64};
    plan->getPipelines()[0]->execute(buffer, workerContext);

    testSink->cleanupBuffers();
    buffer.release();
    plan->stop();
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(); // return ms
}

std::chrono::high_resolution_clock::time_point
startMaintenanceQuery(std::shared_ptr<TestSink> testSink,
                      Runtime::NodeEnginePtr nodeEngine,
                      SchemaPtr csvSchema,
                      uint64_t queryId,
                      uint64_t numBuffersToProcess,
                      std::shared_ptr<MaterializedView<uint64_t, EventRecord>> mv) {

    auto sourceDescriptor = CsvSourceDescriptor::create(csvSchema,
                                                        "/Users/adrianmichalke/workspace/masterthesis/nebulastream/tests/test_data/MV_test.csv",
                                                        ",", 10, numBuffersToProcess, 10, true);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(sourceDescriptor).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    auto sinkOperator = queryPlan->getOperatorByType<LogicalUnaryOperatorNode>()[1];

    auto customPipelineStage = std::make_shared<MaintenanceQuery>(mv);
    auto externalOperator =
            NES::QueryCompilation::PhysicalOperators::PhysicalExternalOperator::create(SchemaPtr(), SchemaPtr(), customPipelineStage);

    sinkOperator->insertBetweenThisAndParentNodes(externalOperator);

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto plan = TestUtils::createTestQueryCompiler()->compileQuery(request)->getExecutableQueryPlan();
    nodeEngine->registerQueryInNodeEngine(plan);

    auto start = std::chrono::high_resolution_clock::now();
    nodeEngine->startQuery(queryId);
    return start;
}

TEST_F(MaterializedViewLogTest, AdhocQueryBenchmarkSerial) {
    // Configuration
    auto numberQuery = 1;
    auto numberOfBuffer = 60;

    std::vector<std::shared_ptr<MaterializedView<uint64_t, EventRecord>>> vec = {//std::make_shared<MaterializedViewTableBasedWindowCentered<uint64_t>>(),
            //std::make_shared<MaterializedViewTableBasedMaterializedViewCentered<uint64_t>>()};
            std::make_shared<MaterializedViewTableBasedAdHocCentered<uint64_t>>()};

    for (auto mv : vec) {
        // Start maintenance query
        auto testSink = std::make_shared<TestSink>(numberOfBuffer, csvSchema, nodeEngine->getBufferManager());

        std::vector<std::chrono::high_resolution_clock::time_point> timeVec {};
        for (int i = 0; i < numberQuery; i++) {
            auto start = startMaintenanceQuery(testSink, nodeEngine, csvSchema, i, 60, mv);
            timeVec.push_back(start);
        }

        //sleep(10);

        auto end = std::chrono::high_resolution_clock::now();
        // calc average runtime
        auto total = 0;
        for (auto i : timeVec) {
            total += std::chrono::duration_cast<std::chrono::milliseconds>(end - i).count();
        }
        for (int i = 0; i < numberQuery; i++) {
            nodeEngine->stopQuery(i);
        }

        auto duration = startAdhocQuery1(nodeEngine, csvSchema, numberQuery +1, mv);

        auto elapsed  = total / numberQuery;
        NES_INFO("Name " << mv->name() << " elapsed: " << elapsed << "ms, MV size: " << mv->size() << " AdHoc elapsed: " << duration << "ms");

        testSink->cleanupBuffers();
    }
}*/