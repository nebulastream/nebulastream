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
#include <binaryen-c.h>
#include <gtest/gtest.h>
#include <Nautilus/Backends/WASM/WASMCompiler.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus {

class WASMExpressionTest : public testing::Test {
  public:
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    Nautilus::Backends::WASM::WASMCompiler wasmCompiler;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WASMExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup WASMExpressionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup WASMExpressionTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down WASMExpressionTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down WASMExpressionTest test class." << std::endl; }
};

Value<> int32AddExpression() {
    Value<Int32> x = (int32_t) 1;
    Value<Int32> y = (int32_t) 2;
    return x + y;
}

TEST_F(WASMExpressionTest, addIntFunctionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return int32AddExpression();
    });
    //std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto wasm = wasmCompiler.lower(ir);
    //BinaryenModulePrint(wasm);
}

Value<> int32SubExpression() {
    Value<Int32> x = (int32_t) 1;
    Value<Int32> y = (int32_t) 2;
    return x - y;
}

TEST_F(WASMExpressionTest, subIntFunctionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return int32SubExpression();
    });
    //std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto wasm = wasmCompiler.lower(ir);
    //BinaryenModulePrint(wasm);
}

Value<> int32MulExpression() {
    Value<Int32> x = (int32_t) 1;
    Value<Int32> y = (int32_t) 2;
    return x * y;
}

TEST_F(WASMExpressionTest, mulIntFunctionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return int32MulExpression();
    });
    //std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto wasm = wasmCompiler.lower(ir);
    //BinaryenModulePrint(wasm);
}

Value<> int32DivExpression() {
    Value<Int32> x = (int32_t) 1;
    Value<Int32> y = (int32_t) 2;
    return x / y;
}

TEST_F(WASMExpressionTest, divIntFunctionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return int32DivExpression();
    });
    //std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto wasm = wasmCompiler.lower(ir);
    //BinaryenModulePrint(wasm);
}

Value<> int32AddArgsExpression(Value<Int32> x) {
    Value<Int32> y = (int32_t) 2;
    return x + y;
}

TEST_F(WASMExpressionTest, addInt32ArgsFunctionTest) {
    Value<Int32> tempx = (int32_t) 1;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return int32AddArgsExpression(tempx);
    });
    //std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto wasm = wasmCompiler.lower(ir);
    //BinaryenModulePrint(wasm);
}

Value<> int64AddArgsExpression(Value<Int64> x) {
    Value<Int64> y = 7l;
    return x + y;
}

TEST_F(WASMExpressionTest, addInt64ArgsFunctionTest) {
    Value<Int64> tempx = (int64_t) 1;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return int64AddArgsExpression(tempx);
    });
    //std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto wasm = wasmCompiler.lower(ir);
    //BinaryenModulePrint(wasm);
}

Value<> booleanOrExpression() {
    Value<Boolean> x = true;
    Value<Boolean> y = false;
    return x || y;
}

TEST_F(WASMExpressionTest, orBooleanFunctionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return booleanOrExpression();
    });
    //std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto wasm = wasmCompiler.lower(ir);
    //BinaryenModulePrint(wasm);
}

Value<> floatDivArgsExpression(Value<Float> x) {
    Value<Float> y = 7.5f;
    return x / y;
}

TEST_F(WASMExpressionTest, divFloatFunctionTest) {
    Value<Float> tempx = 10.7f;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createFloatStamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return floatDivArgsExpression(tempx);
    });
    //std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto wasm = wasmCompiler.lower(ir);
    //BinaryenModulePrint(wasm);
}

Value<> int32GTExpression(Value<Int32> x) {
    Value<Int32> y = 7;
    return x > y;
}

TEST_F(WASMExpressionTest, gtInt32FunctionTest) {
    Value<Int32> tempx = 10;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return int32GTExpression(tempx);
    });
    //std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto wasm = wasmCompiler.lower(ir);
    //BinaryenModulePrint(wasm);
}

Value<> ifExpression(Value<Int32> x) {
    Value<Int32> y = 1;
    if (x > 10) {
        y = 7;
    }
    return y;
}

TEST_F(WASMExpressionTest, ifFunctionTest) {
    Value<Int32> tempx = 11;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return ifExpression(tempx);
    });
    //std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto wasm = wasmCompiler.lower(ir);
    //BinaryenModulePrint(wasm);
}

Value<> loopExpression(Value<Int32> x) {
    Value<Int32> y = 1;
    for (int i = 0; i < x; ++i) {
        y = y + 1;
    }
    return y;
}

TEST_F(WASMExpressionTest, loopFunctionTest) {
    Value<Int32> tempx = 5;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return loopExpression(tempx);
    });
    //std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto wasm = wasmCompiler.lower(ir);
    //BinaryenModulePrint(wasm);
}

}// namespace NES::Nautilus