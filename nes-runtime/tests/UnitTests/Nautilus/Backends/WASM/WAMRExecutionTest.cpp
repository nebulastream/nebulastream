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

#include "Nautilus/Backends/WASM/WAMRRuntime.hpp"
#include "Runtime/BufferManager.hpp"
#include "Runtime/WorkerContext.hpp"
#include "TestUtils/AbstractPipelineExecutionTest.hpp"
#include <API/Schema.hpp>
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus {

class WAMRExecutionTest : public Testing::NESBaseTest, public Runtime::AbstractPipelineExecutionTest {
  public:
    //Runtime::Execution::ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WAMRExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup WAMRExecutionTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_INFO("Setup SelectionPipelineTest test case.");
        //provider = Runtime::Execution::ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        //bm = std::make_shared<Runtime::BufferManager>();
        //wc = std::make_shared<Runtime::WorkerContext>(0, bm, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down WAMRExecutionTest test class."); }
};

const char* wat = "(module\n"
                  " (memory $memory 1)\n"
                  " (export \"memory\" (memory $memory))\n"
                  " (export \"execute\" (func $execute))\n"
                  " (func $execute (result i32)\n"
                  "  i32.const 8\n"
                  "  i32.const 6\n"
                  "  i32.add\n"
                  "  return\n"
                  " )\n"
                  ")";

TEST_P(WAMRExecutionTest, callAddFunction) {
    auto wamrRuntime = std::make_unique<Backends::WASM::WAMRRuntime>();
    auto result = wamrRuntime->run(strlen(wat), const_cast<char*>(wat));
    //ASSERT_EQ(0, result);
}

TEST_P(WAMRExecutionTest, allocateBufferTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Runtime::Execution::Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Runtime::Execution::Expressions::ConstantIntegerExpression>(5);
    auto readF2 = std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>("f1");
    auto equalsExpression = std::make_shared<Runtime::Execution::Expressions::EqualsExpression>(readF1, readF2);
    auto selectionOperator = std::make_shared<Runtime::Execution::Operators::Selection>(equalsExpression);
    scanOperator->setChild(selectionOperator);

    auto emitMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Runtime::Execution::Operators::Emit>(std::move(emitMemoryProviderPtr));
    selectionOperator->setChild(emitOperator);

}

INSTANTIATE_TEST_CASE_P(testEmitOperator,
                        WAMRExecutionTest,
                        ::testing::Values("WASM"),
                        [](const testing::TestParamInfo<WAMRExecutionTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus
