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
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Runtime/BufferManager.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus {

class GPULaunchCompilationTest : public testing::Test, public AbstractCompilationBackendTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TraceTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup TraceTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TraceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TraceTest test class." << std::endl; }

    //    // Takes a Nautilus function, creates the trace, converts it Nautilus IR, and applies all available passes.
    //    std::shared_ptr<NES::Nautilus::IR::IRGraph> createTraceAndApplyPasses(std::function<Value<>()> nautilusFunction,
    //                                                                          bool findSimpleCountedLoops = false) {
    //        (void) findSimpleCountedLoops;
    //        auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([nautilusFunction]() {
    //            return nautilusFunction();
    //        });
    //        auto executionTrace = ssaCreationPhase.apply(std::move(execution));
    //        auto ir = irCreationPhase.apply(executionTrace);
    ////        ir = removeBrOnlyBlocksPass.apply(ir);
    ////        ir =  structuredControlFlowPass.apply(ir, findSimpleCountedLoops);
    //        return ir;
    //    }
};

//Value<> sumLoop() {
//    Value agg = Value(5);
//    for (Value start = 0; start < 100; start = start + 1) {
//        agg = agg + 10;
//    }
//    return agg;
//}
//
//TEST_F(GPULaunchCompilationTest, simpleLoop) {
//    auto ir = createTraceAndApplyPasses(&sumLoop, /* findSimpleCountedLoop */ true);
//    auto& compiler = Backends::CompilationBackendRegistry::getPlugin("MLIR");
//    auto engine = compiler->compile(ir);
//
//    auto function = engine->getInvocableMember<int32_t (*)()>("execute");
//
//    ASSERT_EQ(function(), 105);
//}
//
void elementWiseOperation(Value<MemRef> ptr) {
    auto size = Value((int64_t) 5);
    for (auto i = Value((int64_t) 0); i < size; i = i + (int64_t) 1) {
        auto address = ptr + i * (int64_t) 8;
        auto value = address.as<MemRef>().load<Int64>();
        auto tmp = value * (int64_t) 2;
        address.as<MemRef>().store(tmp);
    }
}

TEST_F(GPULaunchCompilationTest, elementWiseOperationTest) {
    auto memPtr = Value<MemRef>(nullptr);
    memPtr.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());

    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolically([&memPtr]() {
        elementWiseOperation(memPtr);
    });

    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto ir = irCreationPhase.apply(executionTrace);
    removeBrOnlyBlocksPhase.apply(ir);
    loopDetectionPhase.apply(ir);
    structuredControlFlowPhase.apply(ir);

    NES_DEBUG("IR:\n" << ir->toString());

    auto& compiler = Backends::CompilationBackendRegistry::getPlugin("MLIR");
    auto engine = compiler->compile(ir);

    auto function = engine->getInvocableMember<int64_t (*)(void*)>("execute");

    auto array = new int64_t[]{1, 2, 3, 4, 5};
    function(array);
    delete[] array;
}

}// namespace NES::Nautilus