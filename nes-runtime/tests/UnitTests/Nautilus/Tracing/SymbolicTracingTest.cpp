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
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/SymbolicExecution/SymbolicExecutionContext.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus::Tracing {
class SymbolicTracingTest : public testing::Test {
  public:
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SymbolicExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup SymbolicExecutionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup SymbolicExecutionTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down SymbolicExecutionTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down SymbolicExecutionTest test class." << std::endl; }
};

void assignmentOperator() {
    auto iw = Value<>(1);
    auto iw2 = Value<>(2);
    iw = iw2 + iw;
}

TEST_F(SymbolicTracingTest, assignmentOperatorTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        assignmentOperator();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::ADD);
    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::RETURN);
}

void arithmeticExpression() {
    Value iw = Value(1);
    Value iw2 = Value(2);
    Value iw3 = Value(3);
    auto result = iw - iw3 + 2 * iw2 / iw;
}

TEST_F(SymbolicTracingTest, arithmeticExpressionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        arithmeticExpression();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto basicBlocks = executionTrace->getBlocks();

    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];

    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::CONST);

    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::SUB);
    ASSERT_EQ(block0.operations[4].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[5].op, Nautilus::Tracing::MUL);
    ASSERT_EQ(block0.operations[6].op, Nautilus::Tracing::DIV);
    ASSERT_EQ(block0.operations[7].op, Nautilus::Tracing::ADD);
}

void logicalExpressionLessThan() {
    Value iw = Value(1);
    auto result = iw < 2;
}

TEST_F(SymbolicTracingTest, logicalExpressionLessThanTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction(logicalExpressionLessThan);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::LESS_THAN);
}

void logicalExpressionEquals() {
    Value iw = Value(1);
    auto result = iw == 2;
}

TEST_F(SymbolicTracingTest, logicalExpressionEqualsTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        logicalExpressionEquals();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::EQUALS);
}

void logicalExpressionLessEquals() {
    Value iw = Value(1);
    auto result = iw <= 2;
}

TEST_F(SymbolicTracingTest, logicalExpressionLessEqualsTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction(logicalExpressionLessEquals);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::LESS_THAN);
    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::EQUALS);
    ASSERT_EQ(block0.operations[4].op, Nautilus::Tracing::OR);
}

void logicalExpressionGreater() {
    Value iw = Value(1);
    auto result = iw > 2;
}

TEST_F(SymbolicTracingTest, logicalExpressionGreaterTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction(logicalExpressionGreater);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::GREATER_THAN);
}

void logicalExpressionGreaterEquals() {
    Value iw = Value(1);
    auto result = iw >= 2;
}

TEST_F(SymbolicTracingTest, logicalExpressionGreaterEqualsTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction(logicalExpressionGreaterEquals);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::GREATER_THAN);
    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::EQUALS);
    ASSERT_EQ(block0.operations[4].op, Nautilus::Tracing::OR);
}

Value<> logicalAssignTest() {
    Value<Boolean> res = true;
    Value x = Value(1);
    res = res && x == 42;
    Value y = Value(1);
    res = res && y == 42;
    Value z = Value(1);
    return res && z == 42;
}

TEST_F(SymbolicTracingTest, logicalAssignEqualsTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return logicalAssignTest();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    std::cout << *executionTrace.get() << std::endl;
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::CONST);
}

void logicalExpression() {
    Value iw = Value(1);
    auto result = iw == 2 && iw < 1 || true;
}

TEST_F(SymbolicTracingTest, logicalExpressionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        logicalExpression();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));

    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::EQUALS);
    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[4].op, Nautilus::Tracing::LESS_THAN);
    ASSERT_EQ(block0.operations[5].op, Nautilus::Tracing::AND);
    ASSERT_EQ(block0.operations[6].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[7].op, Nautilus::Tracing::OR);
}

void ifCondition(bool flag) {
    Value boolFlag = Value(flag);
    Value iw = Value(1);
    if (boolFlag) {
        iw = iw - 1;
    }
    auto x = iw + 42;
}

TEST_F(SymbolicTracingTest, ifConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolically([]() {
        ifCondition(true);
    });

    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];

    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::CMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 0);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block1.operations[1].op, Nautilus::Tracing::SUB);
    ASSERT_EQ(block1.operations[2].op, Nautilus::Tracing::JMP);
    auto blockref = std::get<Nautilus::Tracing::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments[0], std::get<Nautilus::Tracing::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 0);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Nautilus::Tracing::JMP);

    auto blockref2 = std::get<Nautilus::Tracing::BlockRef>(block2.operations[0].input[0]);
    ASSERT_EQ(blockref2.block, 3);
    ASSERT_EQ(blockref2.arguments.size(), 1);
    ASSERT_EQ(blockref2.arguments[0], block2.arguments[0]);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 1);
    ASSERT_EQ(block3.predecessors[1], 2);
    ASSERT_EQ(block3.arguments.size(), 1);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block3.operations[1].op, Nautilus::Tracing::ADD);
}

void ifElseCondition(bool flag) {
    Value boolFlag = Value(flag);
    Value iw = Value(1);
    Value iw2 = Value(1);
    if (boolFlag) {
        iw = iw - 1;
    } else {
        iw = iw * iw2;
    }
    iw + 1;
}

TEST_F(SymbolicTracingTest, ifElseConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolically([]() {
        ifElseCondition(true);
    });
    std::cout << *executionTrace << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::CMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 0);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block1.operations[1].op, Nautilus::Tracing::SUB);
    ASSERT_EQ(block1.operations[2].op, Nautilus::Tracing::JMP);
    auto blockref = std::get<Nautilus::Tracing::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments[0], std::get<Nautilus::Tracing::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 0);
    ASSERT_EQ(block2.arguments.size(), 2);
    ASSERT_EQ(block2.operations[0].op, Nautilus::Tracing::MUL);
    ASSERT_EQ(block2.operations[1].op, Nautilus::Tracing::JMP);
    auto blockref2 = std::get<Nautilus::Tracing::BlockRef>(block2.operations[1].input[0]);
    ASSERT_EQ(blockref2.block, 3);
    ASSERT_EQ(blockref2.arguments.size(), 1);
    auto resultRef = std::get<Nautilus::Tracing::ValueRef>(block2.operations[0].result);
    ASSERT_EQ(blockref2.arguments[0], resultRef);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 1);
    ASSERT_EQ(block3.predecessors[1], 2);
    ASSERT_EQ(block3.arguments.size(), 1);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block3.operations[1].op, Nautilus::Tracing::ADD);
}

void nestedIfThenElseCondition() {
    Value value = Value(1);
    Value iw = Value(1);
    if (value == 42) {
    } else {
        if (iw == 8) {
        } else {
            iw = iw + 2;
        }
    }
}

TEST_F(SymbolicTracingTest, nestedIfElseConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolically([]() {
        nestedIfThenElseCondition();
    });
    std::cout << *executionTrace << std::endl;

    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 7);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::EQUALS);
    ASSERT_EQ(block0.operations[4].op, Nautilus::Tracing::CMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 0);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::JMP);
    auto blockref = std::get<Nautilus::Tracing::BlockRef>(block1.operations[0].input[0]);
    ASSERT_EQ(blockref.block, 5);

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 0);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block2.operations[1].op, Nautilus::Tracing::EQUALS);
    ASSERT_EQ(block2.operations[2].op, Nautilus::Tracing::CMP);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 2);
    ASSERT_EQ(block3.arguments.size(), 0);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::JMP);

    auto block4 = basicBlocks[4];
    ASSERT_EQ(block4.predecessors[0], 2);
    ASSERT_EQ(block4.arguments.size(), 1);
    ASSERT_EQ(block4.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block4.operations[1].op, Nautilus::Tracing::ADD);
    ASSERT_EQ(block4.operations[2].op, Nautilus::Tracing::JMP);

    auto block5 = basicBlocks[5];
    ASSERT_EQ(block5.predecessors[0], 1);
    ASSERT_EQ(block5.predecessors[1], 3);
    ASSERT_EQ(block5.operations[0].op, Nautilus::Tracing::JMP);
}

void emptyLoop() {
    Value iw = Value(1);
    Value iw2 = Value(2);

    //auto result = t1 + t2;

    //auto res = (iw + icw2);
    for (auto start = iw; start < 2; start = start + 1) {
        std::cout << "loop" << std::endl;
        //iw2 = iw2 + 1;
    }
    auto iw3 = iw2 - 5;
}

TEST_F(SymbolicTracingTest, emptyLoopTest) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolically([]() {
        emptyLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution << std::endl;
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block1.operations[1].op, Nautilus::Tracing::ADD);
    ASSERT_EQ(block1.operations[2].op, Nautilus::Tracing::JMP);
    auto blockref = std::get<Nautilus::Tracing::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 2);
    ASSERT_EQ(blockref.arguments[1], std::get<Nautilus::Tracing::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block2.operations[1].op, Nautilus::Tracing::SUB);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block3.operations[1].op, Nautilus::Tracing::LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, Nautilus::Tracing::CMP);
}

void longEmptyLoop() {
    Value iw = Value(1);
    Value iw2 = Value(2);
    for (auto start = iw; start < 20000; start = start + 1) {
        //std::cout << "loop" << std::endl;
    }
    auto iw3 = iw2 - 5;
}

TEST_F(SymbolicTracingTest, longEmptyLoopTest) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolically([]() {
        longEmptyLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << *execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block1.operations[1].op, Nautilus::Tracing::ADD);
    ASSERT_EQ(block1.operations[2].op, Nautilus::Tracing::JMP);
    auto blockref = std::get<Nautilus::Tracing::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 2);
    ASSERT_EQ(blockref.arguments[1], std::get<Nautilus::Tracing::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block2.operations[1].op, Nautilus::Tracing::SUB);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block3.operations[1].op, Nautilus::Tracing::LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, Nautilus::Tracing::CMP);
}

void sumLoop() {
    Value agg = Value(1);
    for (auto start = Value(0); start < 10; start = start + 1) {
        agg = agg + 1;
    }
    auto res = agg == 10;
}

TEST_F(SymbolicTracingTest, sumLoopTest) {

    auto execution = Nautilus::Tracing::traceFunctionSymbolically([]() {
        sumLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << *execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block1.operations[1].op, Nautilus::Tracing::ADD);
    ASSERT_EQ(block1.operations[2].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block1.operations[3].op, Nautilus::Tracing::ADD);
    ASSERT_EQ(block1.operations[4].op, Nautilus::Tracing::JMP);
    auto blockref = std::get<Nautilus::Tracing::BlockRef>(block1.operations[4].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 2);
    ASSERT_EQ(blockref.arguments[1], std::get<Nautilus::Tracing::ValueRef>(block1.operations[3].result));
    ASSERT_EQ(blockref.arguments[0], std::get<Nautilus::Tracing::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block2.operations[1].op, Nautilus::Tracing::EQUALS);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block3.operations[1].op, Nautilus::Tracing::LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, Nautilus::Tracing::CMP);
}

void sumWhileLoop() {
    Value agg = Value(1);
    while (agg < 20) {
        agg = agg + 1;
    }
    auto res = agg == 10;
}

TEST_F(SymbolicTracingTest, sumWhileLoopTest) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolically([]() {
        sumWhileLoop();
    });

    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << *execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block1.operations[1].op, Nautilus::Tracing::ADD);
    ASSERT_EQ(block1.operations[2].op, Nautilus::Tracing::JMP);
    auto blockref = std::get<Nautilus::Tracing::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 1);
    ASSERT_EQ(blockref.arguments[0], std::get<Nautilus::Tracing::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block2.operations[1].op, Nautilus::Tracing::EQUALS);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 1);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block3.operations[1].op, Nautilus::Tracing::LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, Nautilus::Tracing::CMP);
}

void invertedLoop() {
    Value i = Value(0);
    Value end = Value(300);
    do {
        // body
        i = i + 1;
    } while (i < end);
}

TEST_F(SymbolicTracingTest, invertedLoopTest) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolically([]() {
        invertedLoop();
    });
    std::cout << execution << std::endl;
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::JMP);

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 0);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block3.operations[1].op, Nautilus::Tracing::ADD);
    ASSERT_EQ(block3.operations[2].op, Nautilus::Tracing::LESS_THAN);
    ASSERT_EQ(block3.operations[3].op, Nautilus::Tracing::CMP);
}

void nestedLoop() {
    Value agg = Value(0);
    Value i = Value(0);
    Value end = Value(300);
    for (; i < end; i = i + 1) {
        for (Value j = 0; j < 10; j = j + 1) {
            agg = agg + 1;
        }
    }
}

TEST_F(SymbolicTracingTest, nestedLoop) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolically([]() {
        nestedLoop();
    });
    std::cout << execution << std::endl;
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    std::cout << *execution.get() << std::endl;
    ASSERT_EQ(basicBlocks.size(), 7);
}

void nestedLoopIf() {
    Value agg = Value(0);
    Value i = Value(0);
    Value end = Value(300);
    for (; i < end; i = i + 1) {
        for (Value j = 0; j < 10; j = j + 1) {
            if (agg < 50)
                agg = agg + 1;
            else {
                agg = agg / 2;
            }
        }
    }
}

TEST_F(SymbolicTracingTest, nestedLoopIf) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolically([]() {
        nestedLoopIf();
    });
    std::cout << execution << std::endl;
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    std::cout << *execution.get() << std::endl;
    ASSERT_EQ(basicBlocks.size(), 10);
}

Value<> f2(Value<> x) {
    if (x > 10)
        return x;
    else
        return x + 1;
}

void f1() {
    Value agg = Value(0);
    agg = f2(agg) + 42;
}

TEST_F(SymbolicTracingTest, nestedFunctionCall) {
    auto execution = Nautilus::Tracing::traceFunctionSymbolically([]() {
        f1();
    });
    std::cout << execution << std::endl;
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    std::cout << *execution.get() << std::endl;
    ASSERT_EQ(basicBlocks.size(), 4);
}

}// namespace NES::Nautilus::Tracing