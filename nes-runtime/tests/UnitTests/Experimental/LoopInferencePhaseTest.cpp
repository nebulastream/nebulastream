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

#include <Experimental/IR/Phases/LoopInferencePhase.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
using namespace NES::Nautilus;
namespace NES::Nautilus::IR {

class LoopInferencePhaseTest : public Testing::NESBaseTest {
  public:
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    IR::LoopInferencePhase loopInferencePhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LoopInferencePhaseTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LoopInferencePhaseTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { NES_INFO("Setup LoopInferencePhaseTest test case."); }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Tear down LoopInferencePhaseTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down LoopInferencePhaseTest test class."); }
};

void sumLoop() {
    Value agg = Value(1);
    for (Value start = 0; start < 10; start = start + 1) {
        agg = agg + 43;
    }
    auto res = agg == 10;
}

TEST_F(LoopInferencePhaseTest, sumLoopTest) {

    auto execution = Nautilus::Tracing::traceFunctionSymbolically([]() {
        sumLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    NES_INFO(*execution.get());
    auto ir = irCreationPhase.apply(execution);
    NES_INFO(ir->toString());

    ir = loopInferencePhase.apply(ir);
    NES_INFO(ir->toString());

    auto rootBlock = ir->getRootOperation()->getFunctionBasicBlock();
    ASSERT_TRUE(rootBlock->getTerminatorOp()->getOperationType() == Operations::Operation::LoopOp);
    auto loopOperation = std::static_pointer_cast<Operations::LoopOperation>(rootBlock->getTerminatorOp());
    ASSERT_TRUE(loopOperation->getLoopInfo()->isCountedLoop());
    auto loopInfo = std::dynamic_pointer_cast<Operations::CountedLoopInfo>(loopOperation->getLoopInfo());
    NES_DEBUG(loopOperation);
}

}// namespace NES::Nautilus::IR