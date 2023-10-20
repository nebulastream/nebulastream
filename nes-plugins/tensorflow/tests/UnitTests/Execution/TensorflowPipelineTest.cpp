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

#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Tensorflow/TensorflowInferenceOperator.hpp>
#include <Execution/Operators/Tensorflow/TensorflowInferenceOperatorHandler.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {
class TensorflowPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;
    Nautilus::CompilationOptions options;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TensorflowPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TensorflowPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup TensorflowPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TensorflowPipelineTest test class."); }
};

/**
 * @brief Test running a pipeline containing a threshold window with a Avg aggregation
 */
TEST_P(TensorflowPipelineTest, thresholdWindowWithSum) {

    //Input and output fields
    std::string f1 = "f1";
    std::string f2 = "f2";
    std::string f3 = "f3";
    std::string f4 = "f4";
    auto iris0 = "iris0";
    auto iris1 = "iris1";
    auto iris2 = "iris2";

    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField(f1, BasicType::BOOLEAN);
    scanSchema->addField(f2, BasicType::BOOLEAN);
    scanSchema->addField(f3, BasicType::BOOLEAN);
    scanSchema->addField(f4, BasicType::BOOLEAN);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    std::vector<std::string> inputFields;
    inputFields.emplace_back(f1);
    inputFields.emplace_back(f2);
    inputFields.emplace_back(f3);
    inputFields.emplace_back(f4);

    std::vector<std::string> outputFields;
    outputFields.emplace_back(iris0);
    outputFields.emplace_back(iris1);
    outputFields.emplace_back(iris2);

    auto inferModelOperator = std::make_shared<Operators::TensorflowInferenceOperator>(0, inputFields, outputFields);
    scanOperator->setChild(inferModelOperator);

    //Build emitter
    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField(f1, BasicType::BOOLEAN);
    emitSchema->addField(f2, BasicType::BOOLEAN);
    emitSchema->addField(f3, BasicType::BOOLEAN);
    emitSchema->addField(f4, BasicType::BOOLEAN);
    emitSchema->addField(iris0, BasicType::FLOAT32);
    emitSchema->addField(iris1, BasicType::FLOAT32);
    emitSchema->addField(iris2, BasicType::FLOAT32);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    inferModelOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0][f1].write((bool) false);
    dynamicBuffer[0][f2].write((bool) true);
    dynamicBuffer[0][f3].write((bool) false);
    dynamicBuffer[0][f4].write((bool) true);
    dynamicBuffer[1][f1].write((bool) false);
    dynamicBuffer[1][f2].write((bool) true);
    dynamicBuffer[1][f3].write((bool) false);
    dynamicBuffer[1][f4].write((bool) true);
    dynamicBuffer.setNumberOfTuples(2);

    auto executablePipeline = provider->create(pipeline, options);

    auto handler = std::make_shared<Operators::TensorflowInferenceOperatorHandler>(std::filesystem::path(TEST_DATA_DIRECTORY)
                                                                                   / "iris_95acc.tflite");

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    EXPECT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 2);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, resultBuffer);
    float expectedValue = 0.43428239;
    auto delta = 0.0000001;
    EXPECT_EQ(resultDynamicBuffer[0][iris0].read<float>(), expectedValue);
    EXPECT_NEAR(resultDynamicBuffer[0][iris0].read<float>(), expectedValue, delta);
}

// TODO #3468: parameterize the aggregation function instead of repeating the similar test
INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        TensorflowPipelineTest,
                        ::testing::Values("PipelineInterpreter", "BCInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<TensorflowPipelineTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Runtime::Execution
