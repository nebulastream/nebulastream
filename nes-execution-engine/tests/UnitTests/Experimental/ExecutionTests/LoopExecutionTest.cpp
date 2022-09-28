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

#include <API/Schema.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Experimental/Interpreter/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/Operators/Emit.hpp>
#include <Experimental/Interpreter/Operators/Scan.hpp>
#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Nautilus/Backends/MLIR/MLIRUtility.hpp>
#include <Experimental/NESIR/Phases/LoopInferencePhase.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <execinfo.h>
#include <gtest/gtest.h>
#include <memory>
using namespace NES::Nautilus;
namespace NES::ExecutionEngine::Experimental::Interpreter {
class LoopExecutionTest : public testing::Test {
  public:
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    IR::LoopInferencePhase loopInferencePhase;
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


};

Value<> sumLoop(int upperLimit) {
    Value agg = Value(1);
    for (Value start = 0; start < upperLimit; start = start + 1) {
        agg = agg + 10;
    }
    return agg;
}



TEST_F(LoopExecutionTest, sumLoopTestSCF) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return sumLoop(10);
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution.get() << std::endl;
    auto ir = irCreationPhase.apply(execution);
    std::cout << ir->toString() << std::endl;
    ir = loopInferencePhase.apply(ir);
    std::cout << ir->toString() << std::endl;

    auto engine = MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 101);
}

Value<> nestedSumLoop(int upperLimit) {
    Value agg = Value(1);
    for (Value start = 0; start < upperLimit; start = start + 1) {
        for (Value start2 = 0; start2 < upperLimit; start2 = start2 + 1) {
            agg = agg + 10;
        }
    }
    return agg;
}



TEST_F(LoopExecutionTest, nestedLoopTest) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return nestedSumLoop(10);
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution.get() << std::endl;
    auto ir = irCreationPhase.apply(execution);
    std::cout << ir->toString() << std::endl;
    ir = loopInferencePhase.apply(ir);
    std::cout << ir->toString() << std::endl;

    auto engine = MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 1001);
}

TEST_F(LoopExecutionTest, sumLoopTestCF) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return sumLoop(10);
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution.get() << std::endl;
    auto ir = irCreationPhase.apply(execution);
    std::cout << ir->toString() << std::endl;
    ir = loopInferencePhase.apply(ir);
    std::cout << ir->toString() << std::endl;

    auto engine = MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 101);
}

Value<> ifSumLoop() {
    Value agg = Value(1);
    for (Value start = 0; start < 10; start = start + 1) {
        if (agg < 50) {
            agg = agg + 10;
        }
    }
    return agg;
}

TEST_F(LoopExecutionTest, ifSumLoopTest) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return ifSumLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution.get() << std::endl;
    auto ir = irCreationPhase.apply(execution);
    std::cout << ir->toString() << std::endl;
    ir = loopInferencePhase.apply(ir);
    std::cout << ir->toString() << std::endl;

    auto engine = MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 51);
}

Value<> ifElseSumLoop() {
    Value agg = Value(1);
    for (Value<Int32> start = 0; start < 10; start = start + 1) {
        if (agg < 50) {
            agg = agg + 10;
        } else {
            agg = agg + 1;
        }
    }
    return agg;
}

TEST_F(LoopExecutionTest, ifElseSumLoopTest) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return ifElseSumLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution.get() << std::endl;
    auto ir = irCreationPhase.apply(execution);
    std::cout << ir->toString() << std::endl;
    ir = loopInferencePhase.apply(ir);
    std::cout << ir->toString() << std::endl;

    auto engine = MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 56);
}

Value<> elseOnlySumLoop() {
    Value agg = Value(1);
    for (Value start = 0; start < 10; start = start + 1) {
        if (agg < 50) {
        } else {
            agg = agg + 1;
        }
    }
    return agg;
}

TEST_F(LoopExecutionTest, elseOnlySumLoopTest) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return elseOnlySumLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution.get() << std::endl;
    auto ir = irCreationPhase.apply(execution);
    std::cout << ir->toString() << std::endl;
    ir = loopInferencePhase.apply(ir);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto engine = MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 1);
}

Value<> nestedIfSumLoop() {
    Value agg = Value(1);
    for (Value start = 0; start < 10; start = start + 1) {
        if (agg < 50) {
            if (agg < 40) {
                agg = agg + 10;
            }
        } else {
            agg = agg + 1;
        }
    }
    return agg;
}

TEST_F(LoopExecutionTest, nestedIfSumLoopTest) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return nestedIfSumLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution.get() << std::endl;
    auto ir = irCreationPhase.apply(execution);
    std::cout << ir->toString() << std::endl;
    ir = loopInferencePhase.apply(ir);
    std::cout << ir->toString() << std::endl;

    auto engine = MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 41);
}

Value<> nestedIfElseSumLoop() {
    Value agg = Value(1);
    for (Value start = 0; start < 10; start = start + 1) {
        if (agg < 50) {
            if (agg < 40) {
                agg = agg + 10;
            } else {
                agg = agg + 100;
            }
        } else {
            agg = agg + 1;
        }
    }
    return agg;
}

TEST_F(LoopExecutionTest, nestedIfElseSumLoopTest) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return nestedIfElseSumLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution.get() << std::endl;
    auto ir = irCreationPhase.apply(execution);
    std::cout << ir->toString() << std::endl;
    ir = loopInferencePhase.apply(ir);
    std::cout << ir->toString() << std::endl;

    auto engine = MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 146);
}

Value<> nestedElseOnlySumLoop() {
    Value agg = Value(1);
    for (Value start = 0; start < 10; start = start + 1) {
        if (agg < 50) {
            if (agg < 40) {
            } else {
                agg = agg + 100;
            }
        } else {
            agg = agg + 1;
        }
    }
    return agg;
}

TEST_F(LoopExecutionTest, nestedElseOnlySumLoop) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return nestedElseOnlySumLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution.get() << std::endl;
    auto ir = irCreationPhase.apply(execution);
    std::cout << ir->toString() << std::endl;
    ir = loopInferencePhase.apply(ir);
    std::cout << ir->toString() << std::endl;

    auto engine = MLIR::MLIRUtility::compileNESIRToMachineCode(ir);
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 1);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter