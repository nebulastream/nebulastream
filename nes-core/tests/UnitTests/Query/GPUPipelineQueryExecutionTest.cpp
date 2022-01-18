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
// clang-format: off
#include "gtest/gtest.h"
// clang-format: on
#include "../../util/DummySink.hpp"
#include "../../util/SchemaSourceDescriptor.hpp"
#include "../../util/TestQuery.hpp"
#include "../../util/TestQueryCompiler.hpp"
#include "../../util/TestSink.hpp"
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Network/NetworkChannel.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalExternalOperator.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <utility>

#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>

#include <cuda.h>
#include <cuda_runtime.h>
#include <Util/jitify/jitify.hpp>

using namespace NES;
using Runtime::TupleBuffer;

class QueryExecutionTest : public testing::Test {
  public:
    static void SetUpTestCase() { NES::setupLogging("QueryExecutionTest.log", NES::LOG_DEBUG); }
    /* Will be called before a test is executed. */
    void SetUp() override {
        // create test input buffer
        testSchema = Schema::create()->addField("test$value", BasicType::INT32);
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        nodeEngine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { ASSERT_TRUE(nodeEngine->stop()); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {}

    SchemaPtr testSchema;
    Runtime::NodeEnginePtr nodeEngine;
};

void fillBuffer(TupleBuffer& buf, const Runtime::MemoryLayouts::RowLayoutPtr& memoryLayout) {

    auto valueField = Runtime::MemoryLayouts::RowLayoutField<int32_t, true>::create(0, memoryLayout, buf);

    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        valueField[recordIndex] = recordIndex;
    }
    buf.setNumberOfTuples(10);
}

class GPUPipelineStageExample : public Runtime::Execution::ExecutablePipelineStage {
  public:
    ExecutionResult execute(Runtime::TupleBuffer& buffer,
                            Runtime::Execution::PipelineExecutionContext& ctx,
                            Runtime::WorkerContext& wc) override {
        auto record = buffer.getBuffer<int>();

        const char* const SimpleKernel_cu =
            "SimpleKernel.cu\n"
            "__global__ void simpleAdditionKernel(const int* recordValue, const int count, int* result) {\n"
            "    auto i = blockIdx.x * blockDim.x + threadIdx.x;\n"
            "\n"
            "    if (i < count) {\n"
            "        result[i] = recordValue[i] + 42;\n"
            "    }\n"
            "}\n";

        static jitify::JitCache kernel_cache;
        jitify::Program program = kernel_cache.program(SimpleKernel_cu, 0);

        int* d_record;
        cudaMalloc(&d_record, buffer.getNumberOfTuples() * sizeof(int));
        int* d_result;
        cudaMalloc(&d_result, buffer.getNumberOfTuples() * sizeof(int));

        cudaMemcpy(d_record, record, buffer.getNumberOfTuples() * sizeof(int), cudaMemcpyHostToDevice);
        dim3 grid(1);
        dim3 block(32);
        using jitify::reflection::type_of;
        program.kernel("simpleAdditionKernel")
                       .instantiate()
                       .configure(grid, block)
                       .launch(d_record, buffer.getNumberOfTuples(), d_result);
        cudaMemcpy(record, d_result, buffer.getNumberOfTuples() * sizeof(int), cudaMemcpyDeviceToHost);

        ctx.emitBuffer(buffer, wc);
        return ExecutionResult::Ok;
    }
};

TEST_F(QueryExecutionTest, GPUOperatorQuery) {
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
                                                                 nodeEngine->getQueryManager(),
                                                                 id,
                                                                 numSourceLocalBuffers,
                                                                 std::move(successors));
        });

    auto outputSchema = Schema::create()->addField("value", BasicType::INT32);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).filter(Attribute("value") < 5).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    // add physical operator behind the filter
    auto filterOperator = queryPlan->getOperatorByType<FilterLogicalOperatorNode>()[0];

    auto customPipelineStage = std::make_shared<GPUPipelineStageExample>();
    auto externalOperator =
        NES::QueryCompilation::PhysicalOperators::PhysicalExternalOperator::create(SchemaPtr(), SchemaPtr(), customPipelineStage);

    filterOperator->insertBetweenThisAndParentNodes(externalOperator);

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 2u);
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 4};
    if (auto buffer = nodeEngine->getBufferManager()->getBufferBlocking(); !!buffer) {
        auto memoryLayout =
            Runtime::MemoryLayouts::RowLayout::create(testSchema, nodeEngine->getBufferManager()->getBufferSize());
        fillBuffer(buffer, memoryLayout);
        plan->setup();
        ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Deployed);
        ASSERT_TRUE(plan->start(nodeEngine->getStateManager()));
        ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
        ASSERT_EQ(plan->getPipelines()[1]->execute(buffer, workerContext), ExecutionResult::Ok);

        // This plan should produce one output buffer
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);

        auto resultBuffer = testSink->get(0);
        // The output buffer should contain 5 tuple;
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5u);

        auto valueField = Runtime::MemoryLayouts::RowLayoutField<int32_t, true>::create(0, memoryLayout, resultBuffer);
        for (int recordIndex = 0; recordIndex < 5; ++recordIndex) {
            // id
            EXPECT_EQ(valueField[recordIndex], recordIndex + 42);
        }
    }
    ASSERT_TRUE(plan->stop());
    testSink->cleanupBuffers();
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}