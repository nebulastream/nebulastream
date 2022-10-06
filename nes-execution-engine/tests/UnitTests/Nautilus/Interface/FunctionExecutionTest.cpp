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

#include <Nautilus/Backends/MLIR/MLIRUtility.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus {

class FunctionExecutionTest : public testing::Test {
  public:
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("FunctionExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup FunctionExecutionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TraceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TraceTest test class." << std::endl; }
};

int64_t addInt(int64_t x, int64_t y) { return x + y; };

Value<> addIntFunction() {
    auto x = Value<Int64>((int64_t) 2);
    auto y = Value<Int64>((int64_t) 3);
    Value<Int64> res = FunctionCall<>("add", addInt, x, y);
    return res;
}

TEST_F(FunctionExecutionTest, addIntFunctionTest) {

    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return addIntFunction();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto engine = Backends::MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 5);
}

int64_t returnConst() { return 42; };

Value<> returnConstFunction() { return FunctionCall<>("returnConst", returnConst); }

TEST_F(FunctionExecutionTest, returnConstFunctionTest) {

    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return returnConstFunction();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto engine = Backends::MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 42);
}

void voidException() { NES_THROW_RUNTIME_ERROR("An expected exception"); };

void voidExceptionFunction() { FunctionCall<>("voidException", voidException); }

TEST_F(FunctionExecutionTest, voidExceptionFunctionTest) {

    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolically([]() {
        voidExceptionFunction();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto engine = Backends::MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_ANY_THROW(function());
}

int64_t multiplyArgument(int64_t x) { return x * 10; };

Value<> multiplyArgumentFunction(Value<Int64> x) {
    Value<Int64> res = FunctionCall<>("multiplyArgument", multiplyArgument, x);
    return res;
}

TEST_F(FunctionExecutionTest, multiplyArgumentTest) {
    Value<Int64> tempPara = Value<Int64>((int64_t) 0);
    tempPara.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([&tempPara]() {
        return multiplyArgumentFunction(tempPara);
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto engine = Backends::MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)(int64_t)) engine->lookup("execute").get();
    ASSERT_EQ(function(10), 100);
    ASSERT_EQ(function(42), 420);
}

}// namespace NES::Nautilus