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
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/SymbolicExecution/SymbolicExecutionContext.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/BasicTraceFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus::Tracing {
class SymbolicTracingTest : public Testing::NESBaseTest {
  public:
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SymbolicExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SymbolicExecutionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("Tear down SymbolicExecutionTest test class."); }
};

TEST_F(SymbolicTracingTest, assignmentOperatorTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        assignmentOperator();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::ADD);
    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::RETURN);
}

TEST_F(SymbolicTracingTest, arithmeticExpressionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        arithmeticExpression();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
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

TEST_F(SymbolicTracingTest, logicalNegateTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction(logicalNegate);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::NEGATE);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::RETURN);
}

TEST_F(SymbolicTracingTest, logicalExpressionLessThanTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction(logicalExpressionLessThan);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::LESS_THAN);
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

TEST_F(SymbolicTracingTest, logicalAssignEqualsTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return logicalAssignTest();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    NES_INFO(*executionTrace.get());
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::CONST);
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

TEST_F(SymbolicTracingTest, ifConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        ifCondition(true);
    });

    NES_INFO(*executionTrace.get());
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
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

TEST_F(SymbolicTracingTest, ifElseConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        ifElseCondition(true);
    });
    NES_INFO(*executionTrace);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace);
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

TEST_F(SymbolicTracingTest, nestedIfElseConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        nestedIfThenElseCondition();
    });
    NES_INFO(*executionTrace);

    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace);
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

TEST_F(SymbolicTracingTest, emptyLoopTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        emptyLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    NES_INFO(*execution);
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

TEST_F(SymbolicTracingTest, longEmptyLoopTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        longEmptyLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    NES_INFO(*execution);
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

TEST_F(SymbolicTracingTest, sumLoopTest) {

    auto execution = Nautilus::Tracing::traceFunction([]() {
        sumLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    NES_INFO(*execution);
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

TEST_F(SymbolicTracingTest, sumWhileLoopTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        sumWhileLoop();
    });

    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    NES_INFO(*execution);
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

TEST_F(SymbolicTracingTest, invertedLoopTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        invertedLoop();
    });
    NES_INFO(execution);
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    NES_INFO(execution);
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

TEST_F(SymbolicTracingTest, nestedLoopTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        nestedLoop();
    });
    NES_INFO(execution);
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    NES_INFO(*execution.get());
    ASSERT_EQ(basicBlocks.size(), 7);
}

TEST_F(SymbolicTracingTest, nestedLoopIfTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        nestedLoopIf();
    });
    NES_INFO(execution);
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    NES_INFO(*execution.get());
    ASSERT_EQ(basicBlocks.size(), 10);
}

TEST_F(SymbolicTracingTest, loopWithBreakTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        loopWithBreak();
    });
    NES_INFO(execution);
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    NES_INFO(*execution.get());
    ASSERT_EQ(basicBlocks.size(), 7);
}

TEST_F(SymbolicTracingTest, nestedFunctionCallTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        f1();
    });
    NES_INFO(execution);
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    NES_INFO(*execution.get());
    ASSERT_EQ(basicBlocks.size(), 4);
}

TEST_F(SymbolicTracingTest, deepLoopTest) {

    auto execution = Nautilus::Tracing::traceFunction([]() {
        deepLoop();
    });

    NES_INFO(execution);
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    NES_INFO(*execution.get());
    ASSERT_EQ(basicBlocks.size(), 61);
}

TEST_F(SymbolicTracingTest, tracingBreakerTest) {

    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    auto execution = Nautilus::Tracing::traceFunction([]() {
        TracingBreaker();
    });

    NES_INFO(execution);
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    NES_INFO(*execution.get());
    ASSERT_EQ(basicBlocks.size(), 13);
}

}// namespace NES::Nautilus::Tracing