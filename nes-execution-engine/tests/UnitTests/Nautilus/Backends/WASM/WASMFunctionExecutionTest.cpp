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
#include <Experimental/Interpreter/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Nautilus/Backends/WASM/WASMCompiler.hpp>
#include <Nautilus/Backends/WASM/WASMRuntime.hpp>
#include <API/Schema.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Operators/Emit.hpp>
#include <Experimental/Interpreter/Operators/Scan.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Nautilus/Backends/MLIR/MLIRUtility.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>

using namespace NES::Nautilus;

namespace NES::Nautilus {

class WASMFunctionExecutionTest : public testing::Test {
  public:
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    Nautilus::Backends::WASM::WASMCompiler wasmCompiler;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WASMFunctionExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup WASMFunctionExecutionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup WASMFunctionExecutionTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down v test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down WASMFunctionExecutionTest test class." << std::endl; }
};


TEST_F(WASMFunctionExecutionTest, emitQueryTest) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);

    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::UINT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    Scan scan = Scan(memoryLayout);
    auto emit = std::make_shared<Emit>(memoryLayout);
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
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution.get() << std::endl;
    auto ir = irCreationPhase.apply(execution);
    std::cout << ir->toString() << std::endl;

    auto wasmCompiler = std::make_unique<Backends::WASM::WASMCompiler>();
    auto wasm = wasmCompiler->lower(ir);
    auto proxies = wasmCompiler->getProxyFunctionNames();
    auto engine = std::make_unique<Backends::WASM::WASMRuntime>();
    engine->setup(proxies);

}


}// namespace NES::Nautilus