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

#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus {

class MemoryAccessCompilationTest : public Testing::TestWithErrorHandling, public AbstractCompilationBackendTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MemoryAccessCompilationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup MemoryAccessCompilationTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("Tear down MemoryAccessCompilationTest test class."); }
};

Value<> loadFunction(Value<MemRef> ptr) { return ptr.load<Int64>(); }

TEST_P(MemoryAccessCompilationTest, loadFunctionTest) {
    int64_t valI = 42;
    auto tempPara = Value<MemRef>(std::make_unique<MemRef>((int8_t*) &valI));
    // create fake ref TODO improve handling of parameters
    tempPara.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([&tempPara]() {
        return loadFunction(tempPara);
    });

    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t, void*>("execute");

    ASSERT_EQ(function(&valI), 42);
}

void storeFunction(Value<MemRef> ptr) {
    auto value = ptr.load<Int64>();
    auto tmp = value + 1_s64;
    ptr.store(tmp);
}

TEST_P(MemoryAccessCompilationTest, storeFunctionTest) {
    int64_t valI = 42;
    auto tempPara = Value<MemRef>((int8_t*) &valI);
    tempPara.load<Int64>();
    // create fake ref TODO improve handling of parameters
    tempPara.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
    auto executionTrace = Nautilus::Tracing::traceFunction([&tempPara]() {
        storeFunction(tempPara);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<void, void*>("execute");
    function(&valI);
    ASSERT_EQ(valI, 43);
}

Value<Int64> memScan(Value<MemRef> ptr, Value<Int64> size) {
    Value<Int64> sum(0_s64);
    for (auto i = Value<Int64>(0_s64); i < size; i = i + 1_s64) {
        auto address = ptr + i * 8_s64;
        auto value = address.as<MemRef>().load<Int64>();
        sum = sum + value;
    }
    return sum;
}

TEST_P(MemoryAccessCompilationTest, memScanFunctionTest) {
    auto memPtr = Value<MemRef>(nullptr);
    memPtr.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
    auto size = Value<Int64>(0_s64);
    size.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([&memPtr, &size]() {
        return memScan(memPtr, size);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t, int64_t, void*>("execute");
    auto array = new int64_t[]{1, 2, 3, 4, 5, 6, 7};
    ASSERT_EQ(function(7, array), 28);
}

// Tests all registered compilation backends.
// To select a specific compilation backend use ::testing::Values("MLIR") instead of ValuesIn.
INSTANTIATE_TEST_CASE_P(testLoopCompilation,
                        MemoryAccessCompilationTest,
                        ::testing::ValuesIn(Backends::CompilationBackendRegistry::getPluginNames().begin(),
                                            Backends::CompilationBackendRegistry::getPluginNames().end()),
                        [](const testing::TestParamInfo<MemoryAccessCompilationTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus