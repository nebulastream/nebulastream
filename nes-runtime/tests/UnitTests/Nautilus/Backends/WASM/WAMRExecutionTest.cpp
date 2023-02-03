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

#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <API/Schema.hpp>
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Nautilus/Backends/WASM/WAMRRuntime.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus {

class WAMRExecutionTest : public Testing::NESBaseTest {
  public:
    //Runtime::Execution::ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WAMRExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup WAMRExecutionTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_INFO("Setup SelectionPipelineTest test case.");
        //provider = Runtime::Execution::ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 100);
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

TEST_F(WAMRExecutionTest, callAddFunction) {
    auto wamrRuntime = std::make_unique<Backends::WASM::WAMRRuntime>();
    auto result = wamrRuntime->run(strlen(wat), const_cast<char*>(wat));
    //ASSERT_EQ(0, result);
}

TEST_F(WAMRExecutionTest, allocateBufferTest) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<Runtime::WorkerContext>(0, bm, 100);
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto rowMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto pipelineContext = Runtime::Execution::Operators::MockedPipelineExecutionContext();
    auto memoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(rowMemoryLayout);
    auto emitOperator = Runtime::Execution::Operators::Emit(std::move(memoryProviderPtr));
    auto ctx = Runtime::Execution::ExecutionContext(Value<MemRef>((int8_t*) wc.get()), Value<MemRef>((int8_t*) &pipelineContext));
    auto recordBuffer = Runtime::Execution::RecordBuffer(Value<MemRef>(nullptr));
    emitOperator.open(ctx, recordBuffer);
    auto record = Record({{"f1", Value<>(0)}, {"f2", Value<>(10)}});
    auto execution = Nautilus::Tracing::traceFunction([&ctx, &emitOperator, &record]() {
        emitOperator.execute(ctx, record);
    });
    emitOperator.close(ctx, recordBuffer);
    auto executionTrace = ssaCreationPhase.apply(std::move(execution));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    auto s = ir->toString();
    std::cout << s << std::endl;
    /*
    auto schema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto columnMemoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, bm->getBufferSize());

    auto pipelineExeCxtRef = Value<MemRef>((int8_t*) nullptr);
    pipelineExeCxtRef.ref =
        Nautilus::Tracing::ValueRef(INT32_MAX, 0, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto wcRef = Value<MemRef>((int8_t*) nullptr);
    wcRef.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());

    auto memoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::ColumnMemoryProvider>(columnMemoryLayout);
    auto scanOperator = Runtime::Execution::Operators::Scan(std::move(memoryProviderPtr));
    auto collector = std::make_shared<Runtime::Execution::Operators::CollectOperator>();
    scanOperator.setChild(collector);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(columnMemoryLayout, buffer);
    for (uint64_t i = 0; i < dynamicBuffer.getCapacity(); i++) {
        dynamicBuffer[i]["f1"].write((int64_t) i % 2);
        dynamicBuffer[i]["f2"].write((int64_t) 1);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }
    auto ctx = Runtime::Execution::ExecutionContext(wcRef, pipelineExeCxtRef);
    auto recordBuffer = NES::Runtime::Execution::RecordBuffer(Value<MemRef>((int8_t*) std::addressof(buffer)));

    auto execution = Nautilus::Tracing::traceFunction([&]() {
        int x = 10;
        int y = x + 10;
    });
    */
    /*
    auto bm = std::make_shared<Runtime::BufferManager>(100);

    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::UINT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    auto memoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scan = Runtime::Execution::Operators::Scan(std::move(memoryProviderPtr));
    auto emit = Runtime::Execution::Operators::Emit(std::move(memoryProviderPtr));
    scan.setChild(emit);

    auto memRef = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    RecordBuffer recordBuffer = RecordBuffer(memRef);

    auto memRefPCTX = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    memRefPCTX.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
    auto wctxRefPCTX = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    wctxRefPCTX.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createAddressStamp());
    RuntimeExecutionContext executionContext = RuntimeExecutionContext(memRefPCTX);

    auto execution = Nautilus::Tracing::traceFunctionSymbolically([&scan, &executionContext, &recordBuffer]() {
        scan.open(executionContext, recordBuffer);
        scan.close(executionContext, recordBuffer);
    });

    auto executionTrace = ssaCreationPhase.apply(std::move(execution));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    auto s = ir->toString();
    std::cout << s << std::endl;
     */
}

}// namespace NES::Nautilus
