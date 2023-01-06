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
#include <Nautilus/Backends/WASM/WASMCompiler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <NesBaseTest.hpp>

namespace NES::Runtime::Execution {
using namespace Nautilus;
class RecordBuffer;
}

namespace NES::Nautilus {

class WASMCompilerTest : public Testing::NESBaseTest {
  public:
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    Nautilus::Backends::WASM::WASMCompiler wasmCompiler;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WASMCompilerTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup WASMCompilerTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup WASMCompilerTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down WASMCompilerTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down WASMCompilerTest test class." << std::endl; }
};

Value<> int32AddExpression() {
    Value<Int32> x = (int32_t) 1;
    Value<Int32> y = (int32_t) 2;
    return x + y;
}

TEST_F(WASMCompilerTest, addIntFunctionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return int32AddExpression();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto wasmCompiler = std::make_unique<Backends::WASM::WASMCompiler>();
    wasmCompiler->lower(ir);
}

Value<> int32SubExpression() {
    Value<Int32> x = (int32_t) 1;
    Value<Int32> y = (int32_t) 2;
    return x - y;
}

TEST_F(WASMCompilerTest, subIntFunctionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return int32SubExpression();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    wasmCompiler.lower(ir);
}

Value<> int32MulExpression() {
    Value<Int32> x = (int32_t) 1;
    Value<Int32> y = (int32_t) 2;
    return x * y;
}

TEST_F(WASMCompilerTest, mulIntFunctionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return int32MulExpression();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    wasmCompiler.lower(ir);
}

Value<> int32DivExpression() {
    Value<Int32> x = (int32_t) 1;
    Value<Int32> y = (int32_t) 2;
    return x / y;
}

TEST_F(WASMCompilerTest, divIntFunctionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return int32DivExpression();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    wasmCompiler.lower(ir);
}

Value<> int32AddArgsExpression(Value<Int32> x) {
    Value<Int32> y = (int32_t) 2;
    return x + y;
}

TEST_F(WASMCompilerTest, addInt32ArgsFunctionTest) {
    Value<Int32> tempx = (int32_t) 1;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int32AddArgsExpression(tempx);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    wasmCompiler.lower(ir);
}

Value<> int64AddArgsExpression(Value<Int64> x) {
    Value<Int64> y = 7l;
    return x + y;
}

TEST_F(WASMCompilerTest, addInt64ArgsFunctionTest) {
    Value<Int64> tempx = (int64_t) 1;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int64AddArgsExpression(tempx);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    wasmCompiler.lower(ir);
}

Value<> booleanOrExpression() {
    Value<Boolean> x = true;
    Value<Boolean> y = false;
    return x || y;
}

TEST_F(WASMCompilerTest, orBooleanFunctionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return booleanOrExpression();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    wasmCompiler.lower(ir);
}

Value<> floatDivArgsExpression(Value<Float> x) {
    Value<Float> y = 7.5f;
    return x / y;
}

TEST_F(WASMCompilerTest, divFloatFunctionTest) {
    Value<Float> tempx = 10.7f;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createFloatStamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return floatDivArgsExpression(tempx);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    wasmCompiler.lower(ir);
    
}

Value<> int32GTExpression(Value<Int32> x) {
    Value<Int32> y = 7;
    return x > y;
}
TEST_F(WASMCompilerTest, gtInt32FunctionTest) {
    Value<Int32> tempx = 10;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int32GTExpression(tempx);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    wasmCompiler.lower(ir);
}

Value<> ifExpression(Value<Int32> x) {
    Value<Int32> y = 1;
    if (x > 10) {
        y = 7;
    }
    return y;
}

TEST_F(WASMCompilerTest, ifFunctionTest) {
    Value<Int32> tempx = 11;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return ifExpression(tempx);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    wasmCompiler.lower(ir);
}

Value<> ifExpression2(Value<Int32> x) {
    Value<Int32> y = 1;
    Value<Int32> z = 2;
    if (x > 10) {
        y = 7;
        z = z - 1;
    }
    return y + z;
}

TEST_F(WASMCompilerTest, ifFunction2Test) {
    Value<Int32> tempx = 11;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return ifExpression2(tempx);
    });
    
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    wasmCompiler.lower(ir);
    
}

Value<> ifElseExpression(Value<Int32> x) {
    Value<Int32> y = 0;
    if (x > 10) {
        y = 7;
    } else {
        y = 10;
    }
    return y;
}

TEST_F(WASMCompilerTest, ifElseFunctionTest) {
    Value<Int32> tempx = 11;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return ifElseExpression(tempx);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    wasmCompiler.lower(ir);
}

Value<> ifElseIfExpression(Value<Int32> x) {
    Value<Int32> y = 1;
    if (x > 10) {
        y = 7;
    } else if (x < 10) {
        y = 5;
    }
    return y;
}

TEST_F(WASMCompilerTest, ifElseIfFunctionTest) {
    Value<Int32> tempx = 4;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return ifElseIfExpression(tempx);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    wasmCompiler.lower(ir);
}

Value<> loopExpression(Value<Int32> x) {
    Value<Int32> y = 5;
    for (Value<Int32> i = 0; i < x; i = i + 1) {
        y = y + 1;
    }
    return y;
}

TEST_F(WASMCompilerTest, loopFunctionTest) {
    Value<Int32> tempx = 5;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return loopExpression(tempx);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto wasmCompiler = std::make_unique<Backends::WASM::WASMCompiler>();
    wasmCompiler->lower(ir);
}

Value<> memRefExpression(Value<Int32> y) {
    Value<Int32> x = y;
    auto memRef = Value<MemRef>(std::make_unique<MemRef>(MemRef(nullptr)));
    Runtime::Execution::RecordBuffer recordBuffer = Runtime::Execution::RecordBuffer(memRef);
    /*
    memRef.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
    if (y == 9) {
        memRef.store(x);
    }
    */
    return recordBuffer.getNumRecords();
}

TEST_F(WASMCompilerTest, memRefExpressionTest) {
    Value<Int32> tempx = 6;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return memRefExpression(tempx);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto wasmCompiler = std::make_unique<Backends::WASM::WASMCompiler>();
    wasmCompiler->lower(ir);
}

}// namespace NES::Nautilus