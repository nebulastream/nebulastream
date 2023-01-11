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

#include "Execution/Operators/Emit.hpp"
#include <API/Schema.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Experimental/Runtime/RuntimeExecutionContext.hpp>
#include <Nautilus/Backends/WASM/WASMRuntime.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
//#include <mlir/IR/MLIRContext.h>
//#include <Nautilus/Backends/MLIR/MLIRLoweringProvider.hpp>

namespace NES::Runtime::Execution {
using namespace Nautilus;
class RecordBuffer;
}// namespace NES::Runtime::Execution

namespace NES::Nautilus {

class WASMProxyFunctionTest : public Testing::NESBaseTest {
  public:
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WASMProxyFunctionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup WASMProxyFunctionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down WASMProxyFunctionTest test class."); }
};

Value<> tmpExpression(Value<Int32> y) {
    Value<Int32> x = y;
    auto memRef = Value<MemRef>(std::make_unique<MemRef>(MemRef(nullptr)));
    Runtime::Execution::RecordBuffer recordBuffer = Runtime::Execution::RecordBuffer(memRef);

    return recordBuffer.getNumRecords();
}

TEST_F(WASMProxyFunctionTest, tmpExpressionTest) {
    Value<Int32> tempx = 6;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Tracing::traceFunctionWithReturn([tempx]() {
        return tmpExpression(tempx);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
}

TEST_F(WASMProxyFunctionTest, testtest) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Runtime::Execution::Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto emitMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Runtime::Execution::Operators::Emit>(std::move(emitMemoryProviderPtr));
    scanOperator->setChild(emitOperator);
    /*
    auto executionTrace = Nautilus::Tracing::traceFunction([&scanOperator, &executionContext, &recordBuffer]() {
        scanOperator->open(executionContext, recordBuffer);
        scanOperator->close(executionContext, recordBuffer);
    });
     */
}

}// namespace NES::Nautilus