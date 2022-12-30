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
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus {
class TraceTest : public Testing::NESBaseTest {
  public:
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TraceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TraceTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { NES_INFO("Setup TraceTest test case."); }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Tear down TraceTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TraceTest test class."); }
};

void assignmentOperator() {
    Value<> iw = Value<Int16>((int16_t) 1);
    Value<> iw2 = Value<Int16>((int16_t) 2);
    iw = iw2 + iw;
}

TEST_F(TraceTest, assignmentOperatorTest) {

    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        assignmentOperator();
    });
    NES_INFO(*executionTrace.get());
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

void arithmeticExpression() {
    Value iw = Value<Int64>((int64_t) 1);
    Value iw2 = Value<Int64>((int64_t) 2);
    Value iw3 = Value<Int64>((int64_t) 3);
    auto result = iw - iw3 + (int64_t) 2 * iw2 / iw;
}

TEST_F(TraceTest, arithmeticExpressionTest) {
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

void logicalExpressionLessThan() {
    Value iw = Value(1);
    auto result = iw < 2;
}

TEST_F(TraceTest, logicalExpressionLessThanTest) {
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

void logicalExpressionEquals() {
    Value iw = Value(1);
    auto result = iw == 2;
}

TEST_F(TraceTest, logicalExpressionEqualsTest) {
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

TEST_F(TraceTest, logicalExpressionLessEqualsTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction(logicalExpressionLessEquals);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
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

TEST_F(TraceTest, logicalExpressionGreaterTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction(logicalExpressionGreater);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
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

TEST_F(TraceTest, logicalExpressionGreaterEqualsTest) {
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

void logicalExpression() {
    Value iw = Value(1);
    auto result = iw == 2 && iw < 1 || true;
}

TEST_F(TraceTest, logicalExpressionTest) {
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
    iw + 42;
}

TEST_F(TraceTest, ifConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        ifCondition(true);
        Nautilus::Tracing::getThreadLocalTraceContext()->reset();
        ifCondition(false);
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

TEST_F(TraceTest, ifElseConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunction([]() {
        ifElseCondition(true);
        Nautilus::Tracing::getThreadLocalTraceContext()->reset();
        ifElseCondition(false);
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

void emptyLoop() {
    Value iw = Value(1);
    Value iw2 = Value(2);

    //auto result = t1 + t2;

    //auto res = (iw + icw2);
    for (auto start = iw; start < 2; start = start + 1) {
        NES_INFO("loop");
        //iw2 = iw2 + 1;
    }
    auto iw3 = iw2 - 5;
}

TEST_F(TraceTest, emptyLoopTest) {
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

void longEmptyLoop() {
    Value iw = Value(1);
    Value iw2 = Value(2);
    for (auto start = iw; start < 20000; start = start + 1) {
        //NES_INFO("loop" );
    }
    auto iw3 = iw2 - 5;
}

TEST_F(TraceTest, longEmptyLoopTest) {
    auto execution = Nautilus::Tracing::traceFunction([]() {
        longEmptyLoop();
    });
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

TEST_F(TraceTest, sumLoopTest) {

    auto execution = Nautilus::Tracing::traceFunction([]() {
        sumLoop();
    });
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

TEST_F(TraceTest, sumWhileLoopTest) {
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

void invertedLoop() {
    Value i = Value(0);
    Value end = Value(300);
    do {
        // body
        i = i + 1;
    } while (i < end);
}

TEST_F(TraceTest, invertedLoopTest) {
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

}// namespace NES::Nautilus