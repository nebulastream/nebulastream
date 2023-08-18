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

#include <BaseIntegrationTest.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/SymbolicExecution/SymbolicExecutionContext.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/BasicTraceFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus::Tracing {
class SymbolicTracingTest : public Testing::BaseUnitTest {
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
    std::stringstream executionTraceAsString;
    executionTraceAsString << *executionTrace.get();
    NES_INFO("{}", executionTraceAsString.str());
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::ADD);
    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::OpCode::RETURN);
}

TEST_F(SymbolicTracingTest, arithmeticExpressionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        arithmeticExpression();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::stringstream executionTraceAsString;
    executionTraceAsString << *executionTrace.get();
    NES_INFO("{}", executionTraceAsString.str());
    auto basicBlocks = executionTrace->getBlocks();

    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];

    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::CONST);

    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::OpCode::SUB);
    ASSERT_EQ(block0.operations[4].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[5].op, Nautilus::Tracing::OpCode::MUL);
    ASSERT_EQ(block0.operations[6].op, Nautilus::Tracing::OpCode::DIV);
    ASSERT_EQ(block0.operations[7].op, Nautilus::Tracing::OpCode::ADD);
}

TEST_F(SymbolicTracingTest, logicalNegateTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction(logicalNegate);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::stringstream executionTraceAsString;
    executionTraceAsString << *executionTrace.get();
    NES_INFO("{}", executionTraceAsString.str());
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::NEGATE);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::RETURN);
}

TEST_F(SymbolicTracingTest, logicalExpressionLessThanTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction(logicalExpressionLessThan);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::stringstream executionTraceAsString;
    executionTraceAsString << *executionTrace.get();
    NES_INFO("{}", executionTraceAsString.str());
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::LESS_THAN);
}

TEST_F(SymbolicTracingTest, logicalExpressionEqualsTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        logicalExpressionEquals();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::EQUALS);
}

TEST_F(SymbolicTracingTest, logicalExpressionLessEqualsTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction(logicalExpressionLessEquals);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::LESS_THAN);
    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::OpCode::EQUALS);
    ASSERT_EQ(block0.operations[4].op, Nautilus::Tracing::OpCode::OR);
}

TEST_F(SymbolicTracingTest, logicalExpressionGreaterTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction(logicalExpressionGreater);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::GREATER_THAN);
}

TEST_F(SymbolicTracingTest, logicalExpressionGreaterEqualsTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction(logicalExpressionGreaterEquals);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::GREATER_THAN);
    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::OpCode::EQUALS);
    ASSERT_EQ(block0.operations[4].op, Nautilus::Tracing::OpCode::OR);
}

TEST_F(SymbolicTracingTest, logicalAssignEqualsTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return logicalAssignTest();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    std::stringstream executionTraceAsString;
    executionTraceAsString << *executionTrace.get();
    NES_INFO("{}", executionTraceAsString.str());
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::CONST);
}

TEST_F(SymbolicTracingTest, logicalExpressionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        logicalExpression();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));

    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::EQUALS);
    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[4].op, Nautilus::Tracing::OpCode::LESS_THAN);
    ASSERT_EQ(block0.operations[5].op, Nautilus::Tracing::OpCode::AND);
    ASSERT_EQ(block0.operations[6].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[7].op, Nautilus::Tracing::OpCode::OR);
}

TEST_F(SymbolicTracingTest, ifConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        ifCondition(true);
    });

    std::stringstream executionTraceAsString;
    executionTraceAsString << *executionTrace.get();
    NES_INFO("{}", executionTraceAsString.str());
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::stringstream executionTrace2;
    executionTrace2 << *executionTrace.get();
    NES_INFO("{}", executionTrace2.str());
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];

    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::CMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 0);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block1.operations[1].op, Nautilus::Tracing::OpCode::SUB);
    ASSERT_EQ(block1.operations[2].op, Nautilus::Tracing::OpCode::JMP);
    auto blockref = std::get<Nautilus::Tracing::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments[0], std::get<Nautilus::Tracing::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 0);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Nautilus::Tracing::OpCode::JMP);

    auto blockref2 = std::get<Nautilus::Tracing::BlockRef>(block2.operations[0].input[0]);
    ASSERT_EQ(blockref2.block, 3);
    ASSERT_EQ(blockref2.arguments.size(), 1);
    ASSERT_EQ(blockref2.arguments[0], block2.arguments[0]);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 1);
    ASSERT_EQ(block3.predecessors[1], 2);
    ASSERT_EQ(block3.arguments.size(), 1);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block3.operations[1].op, Nautilus::Tracing::OpCode::ADD);
}

TEST_F(SymbolicTracingTest, ifElseConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        ifElseCondition(true);
    });
    std::stringstream executionTraceString;
    executionTraceString << *executionTrace;
    NES_INFO("{}", executionTraceString.str());
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::stringstream executionTraceString2;
    executionTraceString2 << *executionTrace;
    NES_INFO("{}", executionTraceString2.str());
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::OpCode::CMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 0);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block1.operations[1].op, Nautilus::Tracing::OpCode::SUB);
    ASSERT_EQ(block1.operations[2].op, Nautilus::Tracing::OpCode::JMP);
    auto blockref = std::get<Nautilus::Tracing::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments[0], std::get<Nautilus::Tracing::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 0);
    ASSERT_EQ(block2.arguments.size(), 2);
    ASSERT_EQ(block2.operations[0].op, Nautilus::Tracing::OpCode::MUL);
    ASSERT_EQ(block2.operations[1].op, Nautilus::Tracing::OpCode::JMP);
    auto blockref2 = std::get<Nautilus::Tracing::BlockRef>(block2.operations[1].input[0]);
    ASSERT_EQ(blockref2.block, 3);
    ASSERT_EQ(blockref2.arguments.size(), 1);
    auto resultRef = std::get<Nautilus::Tracing::ValueRef>(block2.operations[0].result);
    ASSERT_EQ(blockref2.arguments[0], resultRef);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 1);
    ASSERT_EQ(block3.predecessors[1], 2);
    ASSERT_EQ(block3.arguments.size(), 1);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block3.operations[1].op, Nautilus::Tracing::OpCode::ADD);
}

TEST_F(SymbolicTracingTest, nestedIfElseConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        nestedIfThenElseCondition();
    });
    std::stringstream executionTraceString;
    executionTraceString << *executionTrace;
    NES_INFO("{}", executionTraceString.str());

    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::stringstream executionTraceString2;
    executionTraceString2 << *executionTrace;
    NES_INFO("{}", executionTraceString2.str());
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 7);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[3].op, Nautilus::Tracing::OpCode::EQUALS);
    ASSERT_EQ(block0.operations[4].op, Nautilus::Tracing::OpCode::CMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 0);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::OpCode::JMP);
    auto blockref = std::get<Nautilus::Tracing::BlockRef>(block1.operations[0].input[0]);
    ASSERT_EQ(blockref.block, 5);

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 0);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block2.operations[1].op, Nautilus::Tracing::OpCode::EQUALS);
    ASSERT_EQ(block2.operations[2].op, Nautilus::Tracing::OpCode::CMP);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 2);
    ASSERT_EQ(block3.arguments.size(), 0);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::OpCode::JMP);

    auto block4 = basicBlocks[4];
    ASSERT_EQ(block4.predecessors[0], 2);
    ASSERT_EQ(block4.arguments.size(), 1);
    ASSERT_EQ(block4.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block4.operations[1].op, Nautilus::Tracing::OpCode::ADD);
    ASSERT_EQ(block4.operations[2].op, Nautilus::Tracing::OpCode::JMP);

    auto block5 = basicBlocks[5];
    ASSERT_EQ(block5.predecessors[0], 1);
    ASSERT_EQ(block5.predecessors[1], 3);
    ASSERT_EQ(block5.operations[0].op, Nautilus::Tracing::OpCode::JMP);
}

TEST_F(SymbolicTracingTest, emptyLoopTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        emptyLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::stringstream executionString;
    executionString << *execution;
    NES_INFO("{}", executionString.str());
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block1.operations[1].op, Nautilus::Tracing::OpCode::ADD);
    ASSERT_EQ(block1.operations[2].op, Nautilus::Tracing::OpCode::JMP);
    auto blockref = std::get<Nautilus::Tracing::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 2);
    ASSERT_EQ(blockref.arguments[1], std::get<Nautilus::Tracing::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block2.operations[1].op, Nautilus::Tracing::OpCode::SUB);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block3.operations[1].op, Nautilus::Tracing::OpCode::LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, Nautilus::Tracing::OpCode::CMP);
}

TEST_F(SymbolicTracingTest, longEmptyLoopTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        longEmptyLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::stringstream executionString;
    executionString << *execution;
    NES_INFO("{}", executionString.str());
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block1.operations[1].op, Nautilus::Tracing::OpCode::ADD);
    ASSERT_EQ(block1.operations[2].op, Nautilus::Tracing::OpCode::JMP);
    auto blockref = std::get<Nautilus::Tracing::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 2);
    ASSERT_EQ(blockref.arguments[1], std::get<Nautilus::Tracing::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block2.operations[1].op, Nautilus::Tracing::OpCode::SUB);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block3.operations[1].op, Nautilus::Tracing::OpCode::LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, Nautilus::Tracing::OpCode::CMP);
}

TEST_F(SymbolicTracingTest, sumLoopTest) {

    auto execution = Nautilus::Tracing::traceFunction([]() {
        sumLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::stringstream executionString;
    executionString << *execution;
    NES_INFO("{}", executionString.str());
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block1.operations[1].op, Nautilus::Tracing::OpCode::ADD);
    ASSERT_EQ(block1.operations[2].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block1.operations[3].op, Nautilus::Tracing::OpCode::ADD);
    ASSERT_EQ(block1.operations[4].op, Nautilus::Tracing::OpCode::JMP);
    auto blockref = std::get<Nautilus::Tracing::BlockRef>(block1.operations[4].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 2);
    ASSERT_EQ(blockref.arguments[1], std::get<Nautilus::Tracing::ValueRef>(block1.operations[3].result));
    ASSERT_EQ(blockref.arguments[0], std::get<Nautilus::Tracing::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block2.operations[1].op, Nautilus::Tracing::OpCode::EQUALS);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block3.operations[1].op, Nautilus::Tracing::OpCode::LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, Nautilus::Tracing::OpCode::CMP);
}

TEST_F(SymbolicTracingTest, sumWhileLoopTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        sumWhileLoop();
    });

    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::stringstream executionString;
    executionString << *execution;
    NES_INFO("{}", executionString.str());
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block1.operations[1].op, Nautilus::Tracing::OpCode::ADD);
    ASSERT_EQ(block1.operations[2].op, Nautilus::Tracing::OpCode::JMP);
    auto blockref = std::get<Nautilus::Tracing::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 1);
    ASSERT_EQ(blockref.arguments[0], std::get<Nautilus::Tracing::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block2.operations[1].op, Nautilus::Tracing::OpCode::EQUALS);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 1);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block3.operations[1].op, Nautilus::Tracing::OpCode::LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, Nautilus::Tracing::OpCode::CMP);
}

TEST_F(SymbolicTracingTest, invertedLoopTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        invertedLoop();
    });
    std::stringstream executionString;
    executionString << execution;
    NES_INFO("{}", executionString.str());
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::stringstream executionString2;
    executionString2 << execution;
    NES_INFO("{}", executionString2.str());
    ASSERT_EQ(block0.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[1].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block0.operations[2].op, Nautilus::Tracing::OpCode::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Nautilus::Tracing::OpCode::JMP);

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 0);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, Nautilus::Tracing::OpCode::CONST);
    ASSERT_EQ(block3.operations[1].op, Nautilus::Tracing::OpCode::ADD);
    ASSERT_EQ(block3.operations[2].op, Nautilus::Tracing::OpCode::LESS_THAN);
    ASSERT_EQ(block3.operations[3].op, Nautilus::Tracing::OpCode::CMP);
}

TEST_F(SymbolicTracingTest, nestedLoopTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        nestedLoop();
    });
    std::stringstream executionString;
    executionString << execution;
    NES_INFO("{}", executionString.str());
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    std::stringstream executionString2;
    executionString2 << *execution.get();
    NES_INFO("{}", executionString2.str());
    ASSERT_EQ(basicBlocks.size(), 7);
}

TEST_F(SymbolicTracingTest, nestedLoopIfTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        nestedLoopIf();
    });
    std::stringstream executionString;
    executionString << execution;
    NES_INFO("{}", executionString.str());
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    std::stringstream executionString2;
    executionString2 << *execution.get();
    NES_INFO("{}", executionString2.str());
    ASSERT_EQ(basicBlocks.size(), 10);
}

TEST_F(SymbolicTracingTest, loopWithBreakTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        loopWithBreak();
    });
    std::stringstream executionString;
    executionString << execution;
    NES_INFO("{}", executionString.str());
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    std::stringstream executionString2;
    executionString2 << *execution.get();
    NES_INFO("{}", executionString2.str());
    ASSERT_EQ(basicBlocks.size(), 7);
}

TEST_F(SymbolicTracingTest, nestedFunctionCallTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        f1();
    });
    std::stringstream executionString;
    executionString << execution;
    NES_INFO("{}", executionString.str());
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    std::stringstream executionString2;
    executionString2 << *execution.get();
    NES_INFO("{}", executionString2.str());
    ASSERT_EQ(basicBlocks.size(), 4);
}

TEST_F(SymbolicTracingTest, deepLoopTest) {

    auto execution = Nautilus::Tracing::traceFunction([]() {
        deepLoop();
    });
    std::stringstream executionString;
    executionString << execution;
    NES_INFO("{}", executionString.str());
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    std::stringstream executionString2;
    executionString2 << *execution.get();
    NES_INFO("{}", executionString2.str());
    ASSERT_EQ(basicBlocks.size(), 61);
}

TEST_F(SymbolicTracingTest, DISABLED_nativeLoopTest) {
    // tracing of native loops fails as nautilus can differentiate nautilus and native loops correctly.
    auto execution = Nautilus::Tracing::traceFunction([]() {
        nativeLoop();
    });
    std::stringstream executionString;
    executionString << execution;
    NES_INFO("{}", executionString.str());
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    std::stringstream executionString2;
    executionString2 << *execution.get();
    NES_INFO("{}", executionString2.str());
    ASSERT_EQ(basicBlocks.size(), 61);
}

TEST_F(SymbolicTracingTest, tracingBreakerTest) {

    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    auto execution = Nautilus::Tracing::traceFunction([]() {
        TracingBreaker();
    });
    std::stringstream executionString;
    executionString << execution;
    NES_INFO("{}", executionString.str());
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    std::stringstream executionString2;
    executionString2 << *execution.get();
    NES_INFO("{}", executionString2.str());
    ASSERT_EQ(basicBlocks.size(), 13);
}

}// namespace NES::Nautilus::Tracing