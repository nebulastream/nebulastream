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
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Arrow/ArrowRecordBatchScan.hpp>
#include <Execution/Operators/Arrow/RecordBufferWrapper.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {

class ArrowScanEmitPipelineTest : public Testing::TestWithErrorHandling, public AbstractPipelineExecutionTest {
  public:
    Nautilus::CompilationOptions options;
    ExecutablePipelineProvider* provider{};
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ArrowScanEmitPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ArrowScanEmitPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        NES_INFO("Setup ArrowScanEmitPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        options.setDumpToConsole(true);
        options.setDumpToFile(true);
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ArrowScanEmitPipelineTest test class."); }
};

/**
 * @brief Emit operator that emits a row oriented tuple buffer.
 */
TEST_P(ArrowScanEmitPipelineTest, scanEmitPipeline) {

    auto schema = Schema::create()
                      ->addField("field_boolean", BasicType::BOOLEAN)
                      ->addField("field_int8", BasicType::INT8)
                      ->addField("field_int16", BasicType::INT16)
                      ->addField("field_int32", BasicType::INT32)
                      ->addField("field_int64", BasicType::INT64)
                      ->addField("field_uint8", BasicType::UINT8)
                      ->addField("field_uint16", BasicType::UINT16)
                      ->addField("field_uint32", BasicType::UINT32)
                      ->addField("field_uint64", BasicType::UINT64)
                      ->addField("field_float", BasicType::FLOAT32)
                      ->addField("field_double", BasicType::FLOAT64)
                      ->addField("field_string", BasicType::TEXT);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::ArrowRecordBatchScan>(schema);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    scanOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto executablePipeline = provider->create(pipeline, options);

    auto pipelineContext = MockedPipelineExecutionContext();
    executablePipeline->setup(pipelineContext);

    arrow::io::IOContext io_context = arrow::io::default_io_context();
    std::shared_ptr<arrow::io::InputStream> input =
        arrow::io::ReadableFile::Open(std::string(TEST_DATA_DIRECTORY) + "arrow_test.arrow").ValueOrDie();
    auto recordBatchStreamReader = arrow::ipc::RecordBatchStreamReader::Open(input).ValueUnsafe();
    for (auto batch : MakeIteratorFromReader(recordBatchStreamReader)) {
        auto wrapper = std::make_unique<Operators::RecordBufferWrapper>(batch.MoveValueUnsafe());
        auto tb = Runtime::TupleBuffer::wrapPtr(std::move(wrapper));
        executablePipeline->execute(tb, pipelineContext, *wc);
    }

    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 4);
    auto resultBuffer = pipelineContext.buffers[0];

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    ASSERT_EQ(resultDynamicBuffer.getNumberOfTuples(), 8);
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        ArrowScanEmitPipelineTest,
                        ::testing::Values("PipelineInterpreter", "BCInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<ArrowScanEmitPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution