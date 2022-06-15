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
#include <Experimental/Interpreter/DataValue/MemRef.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Experimental/Interpreter/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/Operators/Aggregation.hpp>
#include <Experimental/Interpreter/Operators/AggregationFunction.hpp>
#include <Experimental/Interpreter/Operators/Emit.hpp>
#include <Experimental/Interpreter/Operators/Scan.hpp>
#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Experimental/NESIR/Phases/LoopInferencePhase.hpp>
#include <Experimental/Trace/ExecutionTrace.hpp>
#include <Experimental/Trace/Phases/SSACreationPhase.hpp>
#include <Experimental/Trace/Phases/TraceToIRConversionPhase.hpp>
#include <Experimental/Trace/TraceContext.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <execinfo.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::ExecutionEngine::Experimental::Interpreter {
class IfExecutionTest : public testing::Test {
  public:
    Trace::SSACreationPhase ssaCreationPhase;
    Trace::TraceToIRConversionPhase irCreationPhase;
    IR::LoopInferencePhase loopInferencePhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("IfExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup IfExecutionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TraceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TraceTest test class." << std::endl; }


};


Value<> ifThenCondition() {
    Value value = 1;
    Value iw = 1;
    if (value == 42) {
        iw = iw + 1;
    }
    return iw + 42;
}

TEST_F(IfExecutionTest, ifConditionTest) {
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return ifThenCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, ir->getIsSCF());
    auto engine = mlirUtility->prepareEngine();
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 43);
}

Value<> ifThenElseCondition() {
    Value value = Value(1);
    Value iw = Value(1);
    if (value == 42) {
        iw = iw + 1;
    } else {
        iw = iw + 42;
    }
    return iw + 42;
}

TEST_F(IfExecutionTest, ifThenElseConditionTest) {
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
       return ifThenElseCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, true);
    auto engine = mlirUtility->prepareEngine();
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 85);
}

Value<> nestedIfThenElseCondition() {
    Value value = Value(1);
    Value iw = Value(1);
    if (value == 42) {
    } else {
        if (iw == 8) {
        } else {
            iw = iw + 2;
        }
    }
   return iw = iw + 2;
}

TEST_F(IfExecutionTest, nestedIFThenElseConditionTest) {
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return nestedIfThenElseCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, ir->getIsSCF());
    auto engine = mlirUtility->prepareEngine();
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 5);
}

Value<> nestedIfNoElseCondition() {
    Value value = Value(1);
    Value iw = Value(1);
    if (value == 42) {
        iw = iw + 4;
    } else {
        iw = iw + 9;
        if (iw == 8) {
            iw + 14;
        }
    }
   return iw = iw + 2;
}

TEST_F(IfExecutionTest, nestedIFThenNoElse) {
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return nestedIfNoElseCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, ir->getIsSCF());
    auto engine = mlirUtility->prepareEngine();
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 12);
}

Value<> doubleIfCondition() {
    Value value = Value(1);
    Value iw = Value(1);
    if (iw == 8) {
        // iw = iw + 14;
    }
    if (iw == 1) {
        iw = iw + 20;
    } 
   return iw = iw + 2;
}

TEST_F(IfExecutionTest, doubleIfCondition) {
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return doubleIfCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, ir->getIsSCF());
    auto engine = mlirUtility->prepareEngine();
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 23);
}

Value<> ifElseIfCondition() {
    Value value = Value(1);
    Value iw = Value(1);
    if (iw == 8) {
        iw = iw + 14;
    }
    else if (iw == 1) {
        iw = iw + 20;
    } 
   return iw = iw + 2;
}

TEST_F(IfExecutionTest, ifElseIfCondition) {
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return ifElseIfCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, ir->getIsSCF());
    auto engine = mlirUtility->prepareEngine();
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 23);
}

Value<> deeplyNestedIfElseCondition() {
    Value value = Value(1);
    Value iw = Value(5);
    if (iw < 8) {
        if (iw > 6) {
            iw = iw + 10;
        } else {
            if (iw < 6) {
                if (iw == 5) {
                    iw = iw + 5;
                }
            } 
        }
    }
    else {
        iw = iw + 20;
    } 
   return iw = iw + 2;
}

TEST_F(IfExecutionTest, DISABLED_deeplyNestedIfElseCondition) {
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return deeplyNestedIfElseCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    // Todo print fails
    // std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, ir->getIsSCF());
    auto engine = mlirUtility->prepareEngine();
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 12);
}

Value<> deeplyNestedIfElseIfCondition() {
    Value value = Value(1);
    Value iw = Value(5);
    if (iw < 8) {
            iw = iw + 10;
    }
    else {    
        if (iw == 5) {
            iw = iw + 5;
        } else if (iw == 4) {
            iw = iw + 4;
        }
    } 
   return iw = iw + 2;
}

// Currently fails, because an empty block (Block 7) is created, during trace building.
TEST_F(IfExecutionTest, DISABLED_deeplyNestedIfElseIfCondition) {
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return deeplyNestedIfElseIfCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    // Todo print fails
    // std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, ir->getIsSCF());
    auto engine = mlirUtility->prepareEngine();
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 12);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter