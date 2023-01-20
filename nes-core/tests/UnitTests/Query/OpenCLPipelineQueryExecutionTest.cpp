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

#include <API/QueryAPI.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <NesBaseTest.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalExternalOperator.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/TestQuery.hpp>
#include <Util/TestQueryCompiler.hpp>
#include <Util/TestSink.hpp>
#include <Util/TestUtils.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

using namespace NES;

class OpenCLQueryExecutionTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("OpenCLQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG); }

  protected:
    void fillInputBuffer(Runtime::TupleBuffer& tupleBuffer,
                         const Runtime::MemoryLayouts::RowLayoutPtr& memoryLayout,
                         unsigned numberOfTuples) {
        // Insert input data into the tuple buffer.
        auto idField = Runtime::MemoryLayouts::RowLayoutField<int32_t, true>::create(0, memoryLayout, tupleBuffer);
        auto valueField = Runtime::MemoryLayouts::RowLayoutField<int32_t, true>::create(0, memoryLayout, tupleBuffer);
        for (auto i = 0u; i < numberOfTuples; ++i) {
            idField[i] = i;
            valueField[i] = 1;
        }
        tupleBuffer.setNumberOfTuples(numberOfTuples);
    }
};

class SimpleOpenCLPipelineStage : public Runtime::Execution::ExecutablePipelineStage {

    class InputRecord {
      public:
        [[maybe_unused]] int32_t id;
        [[maybe_unused]] int32_t value;
    };

    class OutputRecord {
      public:
        [[maybe_unused]] int32_t id;
        [[maybe_unused]] int32_t value;
        [[maybe_unused]] int32_t new1;
        [[maybe_unused]] int32_t new2;
    };

    uint32_t setup(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override {
        // Get kernel source
        // Get OpenCL platform and device
        // Compile kernel
        // Create memory buffers
        // Done
        return ExecutablePipelineStage::setup(pipelineExecutionContext);
    }

    ExecutionResult execute([[maybe_unused]] Runtime::TupleBuffer& buffer,
                            [[maybe_unused]] Runtime::Execution::PipelineExecutionContext& ctx,
                            [[maybe_unused]] Runtime::WorkerContext& wc) override {
        // Obtain an output buffer.
        auto outputBuffer = wc.allocateTupleBuffer();

        // This is a map kernel, so the number of output tuples == number of input tuples.
        outputBuffer.setNumberOfTuples(buffer.getNumberOfTuples());

        // TODO Copy memory buffers
        // TODO Setup kernel call
        // TODO Execute kernel

        // Emit the output buffer and return OK.
        ctx.emitBuffer(outputBuffer, wc);
        return ExecutionResult::Ok;
    }

    uint32_t stop(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override {
        // Cleanup OpenCL resources
        // Done
        return ExecutablePipelineStage::stop(pipelineExecutionContext);
    }
};

TEST_F(OpenCLQueryExecutionTest, simpleOpenCLKernel) {
    // Create an external operator with the custom OpenCL pipeline stage.
    auto customPipelineStage = std::make_shared<SimpleOpenCLPipelineStage>();
    // TODO Can this be nullptr?
    auto externalOperator =
        NES::QueryCompilation::PhysicalOperators::PhysicalExternalOperator::create(SchemaPtr(), SchemaPtr(), customPipelineStage);

    // Create a worker configuration for the node engine and configure a physical source.
    auto workerConfiguration = WorkerConfiguration::create();
    auto sourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "default1", sourceType);
    workerConfiguration->physicalSources.add(physicalSource);

    // Create a node engine to execute the OpenCL pipeline.
    // TODO Is the query status listener necessary?
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                          .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                          .build();

    // Create the input schema with two uint32 fields.
    // TODO Why do the fields have to be prefixed with test$? If this is omitted, type inference below fails.
    auto inputSchema = Schema::create()->addField("test$id", BasicType::INT32)->addField("test$value", BasicType::INT32);

    // Create a source for the test.
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        inputSchema,
        [&](OperatorId id,
            OriginId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createDefaultDataSourceWithSchemaForOneBuffer(inputSchema,
                                                                 nodeEngine->getBufferManager(),
                                                                 nodeEngine->getQueryManager(),
                                                                 id,
                                                                 0,
                                                                 numSourceLocalBuffers,
                                                                 std::move(successors));
        });

    // Create an output schema with the original fields and two new fields.
    auto outputSchema = Schema::create()
                            ->addField("id", BasicType::INT32)
                            ->addField("value", BasicType::INT32)
                            ->addField("new1", BasicType::INT32)
                            ->addField("new2", BasicType::INT32);

    // Create a sink for the test.
    const auto numberOfTuples = 10;
    auto testSink = std::make_shared<TestSink>(numberOfTuples, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    // Create a dummy query into which we can execute the custom pipeline.
    // TODO Why not use from(Schema) method?
    // TODO Can we do this without the filter?
    auto query = TestQuery::from(testSourceDescriptor).filter(Attribute("id") < 5).sink(testSinkDescriptor);

    // Insert the custom pipeline stage into the query.
    auto typeInferencePhase =
        Optimizer::TypeInferencePhase::create(std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr()),
                                              Catalogs::UDF::UdfCatalog::create());
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto filterOperator = queryPlan->getOperatorByType<FilterLogicalOperatorNode>()[0];
//    filterOperator->insertBetweenThisAndParentNodes(externalOperator);

    // Create an executable query plan.
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);

    // The plan should have two pipelines.
//    ASSERT_EQ(plan->getPipelines().size(), 2u);

    // Create a worker context to execute the pipeline.
    // TODO What are these magic numbers?
    auto workerContext = Runtime::WorkerContext(1, nodeEngine->getBufferManager(), 4);

    // Get an input buffer from the buffer manager.
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    // !! converts to bool.
    // TupleBuffer has operator!() which tests if the internal ptr is set.
    // TODO This is bad design, it should be a explanatory method name, e.g., isValid.
    ASSERT_TRUE(!!buffer);

    // Fill the input buffer.
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, nodeEngine->getBufferManager()->getBufferSize());
    fillInputBuffer(buffer, memoryLayout, numberOfTuples);

    // Start the query plan.
    plan->setup();
//    ASSERT_TRUE(plan->setup());
//    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Deployed);
//    ASSERT_TRUE(plan->start(nodeEngine->getStateManager()));
//    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
//    // Execute the second pipeline, which is the custom pipeline?
//    // TODO Why is it the second?
//    ASSERT_EQ(plan->getPipelines()[1]->execute(buffer, workerContext), ExecutionResult::Ok);

}