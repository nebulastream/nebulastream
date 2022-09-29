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

#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Backends/MLIR/MLIRUtility.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

using namespace NES::Nautilus;
namespace NES::Nautilus {
    
class MemoryAccessExecutionTest : public testing::Test {
  public:
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MemoryAccessExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup MemoryAccessExecutionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup MemoryAccessExecutionTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down MemoryAccessExecutionTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down MemoryAccessExecutionTest test class." << std::endl; }
};

Value<> loadFunction(Value<MemRef> ptr) { return ptr.load<Int64>(); }

TEST_F(MemoryAccessExecutionTest, loadFunctionTest) {
    int64_t valI = 42;
    auto tempPara = Value<MemRef>(std::make_unique<MemRef>((int8_t*) &valI));
    // create fake ref TODO improve handling of parameters
    tempPara.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([&tempPara]() {
        return loadFunction(tempPara);
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    auto engine = Backends::MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)(void*)) engine->lookup("execute").get();
    ASSERT_EQ(function(&valI), 42);
}

void storeFunction(Value<MemRef> ptr) {
    auto value = ptr.load<Int64>();
    auto tmp = value + 1l;
    ptr.store(tmp);
}

TEST_F(MemoryAccessExecutionTest, storeFunctionTest) {
    int64_t valI = 42;
    auto tempPara = Value<MemRef>((int8_t*) &valI);
    tempPara.load<Int64>();
    // create fake ref TODO improve handling of parameters
    tempPara.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolically([&tempPara]() {
        storeFunction(tempPara);
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    auto engine = Backends::MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)(void*)) engine->lookup("execute").get();
    function(&valI);
    ASSERT_EQ(valI, 43);
}

Value<Int64> memScan(Value<MemRef> ptr, Value<Int64> size) {
    Value<Int64> sum = 0l;
    for (auto i = Value(0l); i < size; i = i + 1l) {
        auto address = ptr + i * 8l;
        auto value = address.as<MemRef>().load<Int64>();
        sum = sum + value;
    }
    return sum;
}

TEST_F(MemoryAccessExecutionTest, memScanFunctionTest) {
    auto memPtr = Value<MemRef>(nullptr);
    memPtr.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
    auto size = Value<Int64>(0l);
    size.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([&memPtr, &size]() {
        return memScan(memPtr, size);
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    auto engine = Backends::MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)(int, void*)) engine->lookup("execute").get();

    auto array = new int64_t[]{1, 2, 3, 4, 5, 6, 7};
    ASSERT_EQ(function(7, array), 28);
}

}// namespace NES::Nautilus