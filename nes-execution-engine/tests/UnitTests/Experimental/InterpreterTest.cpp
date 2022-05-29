/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law  Trace::OR agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES  Trace::OR CONDITIONS OF ANY KIND, either express  Trace::OR implied.
    See the License for the specific language governing permissions  Trace::AND
    limitations under the License.
*/

#include <API/Schema.hpp>
#include <Experimental/Interpreter/DataValue/Integer.hpp>
#include <Experimental/Interpreter/DataValue/MemRef.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Expressions/EqualsExpression.hpp>
#include <Experimental/Interpreter/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/Operators/Emit.hpp>
#include <Experimental/Interpreter/Operators/Scan.hpp>
#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Experimental/Trace/SSACreationPhase.hpp>
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

class InterpreterTest : public testing::Test {
  public:
    Trace::SSACreationPhase ssaCreationPhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("InterpreterTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup InterpreterTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup InterpreterTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down InterpreterTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down InterpreterTest test class." << std::endl; }
};
/*
uint64_t callNoArgs() { return 42; }

uint64_t   Trace::ADD(uint64_t x, uint64_t y) { return x + y; }
class TestObject {
  public:
    static uint64_t staticFunc(uint64_t x, uint64_t y) { return x + y; }
    uint64_t memberFunc(uint64_t x, uint64_t y) { return x + y; }
};

TEST_F(InterpreterTest, functionCallTest) {

    auto intValue = std::make_unique<Integer>(42);

    std::unique_ptr<Any> anyValue = std::move(intValue);

    auto value = FunctionCall(callNoArgs);
    ASSERT_EQ(value, 42);
    auto res = FunctionCall(  Trace::ADD, (uint64_t) 10, (uint64_t) 11);
    ASSERT_EQ(res, 21);

    res = FunctionCall(TestObject::staticFunc, (uint64_t) 11, (uint64_t) 11);
    ASSERT_EQ(res, 22);

    auto t = TestObject();
    //std::bind(&TestObject::memberFunc, &t, 15, 15);
    uint64_t v = 15;
    TraceContext tc = TraceContext();
    Value value2 = toValue(2, &tc);
    res = bind2(&TestObject::memberFunc, &t, value2, (uint64_t) 15);
   // auto memberFunc = std::mem_fn(&TestObject::memberFunc);
    // res = memberFunc(&t, (uint64_t) 15, (uint64_t) 15);
    ASSERT_EQ(res, 17);
}
*/

void assignmentOperator() {
    auto iw = Value<>(1);
    auto iw2 = Value<>(2);
    iw = iw2 + iw;
}

TEST_F(InterpreterTest, assignmentOperatorTest) {
    auto executionTrace = Trace::traceFunction([]() {
        assignmentOperator();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::CONST);
    ASSERT_EQ(block0.operations[2].op, Trace::ADD);
    ASSERT_EQ(block0.operations[3].op, Trace::RETURN);
}

void arithmeticExpression() {
    Value iw = Value(1);
    Value iw2 = Value(2);
    Value iw3 = Value(3);
    auto result = iw - iw3 + 2 * iw2 / iw;
}

TEST_F(InterpreterTest, arithmeticExpressionTest) {
    auto executionTrace = Trace::traceFunction([]() {
        arithmeticExpression();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto basicBlocks = executionTrace->getBlocks();

    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];

    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::CONST);
    ASSERT_EQ(block0.operations[2].op, Trace::CONST);

    ASSERT_EQ(block0.operations[3].op, Trace::SUB);
    ASSERT_EQ(block0.operations[4].op, Trace::CONST);
    ASSERT_EQ(block0.operations[5].op, Trace::MUL);
    ASSERT_EQ(block0.operations[6].op, Trace::DIV);
    ASSERT_EQ(block0.operations[7].op, Trace::ADD);
}

void logicalExpressionLessThan() {
    Value iw = Value(1);
    auto result = iw < 2;
}

TEST_F(InterpreterTest, logicalExpressionLessThanTest) {
    auto executionTrace = Trace::traceFunction(logicalExpressionLessThan);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::CONST);
    ASSERT_EQ(block0.operations[2].op, Trace::LESS_THAN);
}

void logicalExpressionEquals() {
    Value iw = Value(1);
    auto result = iw == 2;
}

TEST_F(InterpreterTest, logicalExpressionEqualsTest) {
    auto executionTrace = Trace::traceFunction([]() {
        logicalExpressionEquals();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::CONST);
    ASSERT_EQ(block0.operations[2].op, Trace::EQUALS);
}

void logicalExpressionLessEquals() {
    Value iw = Value(1);
    auto result = iw <= 2;
}

TEST_F(InterpreterTest, logicalExpressionLessEqualsTest) {
    auto executionTrace = Trace::traceFunction(logicalExpressionLessEquals);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::CONST);
    ASSERT_EQ(block0.operations[2].op, Trace::LESS_THAN);
    ASSERT_EQ(block0.operations[3].op, Trace::EQUALS);
    ASSERT_EQ(block0.operations[4].op, Trace::OR);
}

void logicalExpressionGreater() {
    Value iw = Value(1);
    auto result = iw > 2;
}

TEST_F(InterpreterTest, logicalExpressionGreaterTest) {
    auto executionTrace = Trace::traceFunction(logicalExpressionGreater);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::CONST);
    ASSERT_EQ(block0.operations[2].op, Trace::LESS_THAN);
    ASSERT_EQ(block0.operations[3].op, Trace::EQUALS);
    ASSERT_EQ(block0.operations[4].op, Trace::OR);
    ASSERT_EQ(block0.operations[5].op, Trace::NEGATE);
}

void logicalExpressionGreaterEquals() {
    Value iw = Value(1);
    auto result = iw >= 2;
}

TEST_F(InterpreterTest, logicalExpressionGreaterEqualsTest) {
    auto executionTrace = Trace::traceFunction(logicalExpressionGreaterEquals);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::CONST);
    ASSERT_EQ(block0.operations[2].op, Trace::LESS_THAN);
    ASSERT_EQ(block0.operations[3].op, Trace::NEGATE);
}

void logicalExpression() {
    Value iw = Value(1);
    auto result = iw == 2 && iw < 1 || true;
}

TEST_F(InterpreterTest, logicalExpressionTest) {
    auto executionTrace = Trace::traceFunction([]() {
        logicalExpression();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));

    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::CONST);
    ASSERT_EQ(block0.operations[2].op, Trace::EQUALS);
    ASSERT_EQ(block0.operations[3].op, Trace::CONST);
    ASSERT_EQ(block0.operations[4].op, Trace::LESS_THAN);
    ASSERT_EQ(block0.operations[5].op, Trace::AND);
    ASSERT_EQ(block0.operations[6].op, Trace::CONST);
    ASSERT_EQ(block0.operations[7].op, Trace::OR);
}

void ifCondition(bool flag) {
    Value boolFlag = Value(flag);
    Value iw = Value(1);
    if (boolFlag) {
        iw = iw - 1;
    }
    iw + 42;
}

TEST_F(InterpreterTest, ifConditionTest) {
    auto executionTrace = Trace::traceFunction([]() {
        ifCondition(true);
        Trace::getThreadLocalTraceContext()->reset();
        ifCondition(false);
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];

    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::CONST);
    ASSERT_EQ(block0.operations[2].op, Trace::CMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 0);
    ASSERT_EQ(block1.operations[0].op, Trace::CONST);
    ASSERT_EQ(block1.operations[1].op, Trace::SUB);
    ASSERT_EQ(block1.operations[2].op, Trace::JMP);
    auto blockref = std::get<Trace::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments[0], std::get<Trace::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 0);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Trace::JMP);

    auto blockref2 = std::get<Trace::BlockRef>(block2.operations[0].input[0]);
    ASSERT_EQ(blockref2.block, 3);
    ASSERT_EQ(blockref2.arguments.size(), 1);
    ASSERT_EQ(blockref2.arguments[0], block2.arguments[0]);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 1);
    ASSERT_EQ(block3.predecessors[1], 2);
    ASSERT_EQ(block3.arguments.size(), 1);
    ASSERT_EQ(block3.operations[0].op, Trace::CONST);
    ASSERT_EQ(block3.operations[1].op, Trace::ADD);
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

TEST_F(InterpreterTest, ifElseConditionTest) {
    auto executionTrace = Trace::traceFunction([]() {
        ifElseCondition(true);
        Trace::getThreadLocalTraceContext()->reset();
        ifElseCondition(false);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace << std::endl;
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::CONST);
    ASSERT_EQ(block0.operations[2].op, Trace::CONST);
    ASSERT_EQ(block0.operations[3].op, Trace::CMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 0);
    ASSERT_EQ(block1.operations[0].op, Trace::CONST);
    ASSERT_EQ(block1.operations[1].op, Trace::SUB);
    ASSERT_EQ(block1.operations[2].op, Trace::JMP);
    auto blockref = std::get<Trace::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments[0], std::get<Trace::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 0);
    ASSERT_EQ(block2.arguments.size(), 2);
    ASSERT_EQ(block2.operations[0].op, Trace::MUL);
    ASSERT_EQ(block2.operations[1].op, Trace::JMP);
    auto blockref2 = std::get<Trace::BlockRef>(block2.operations[1].input[0]);
    ASSERT_EQ(blockref2.block, 3);
    ASSERT_EQ(blockref2.arguments.size(), 1);
    auto resultRef = std::get<Trace::ValueRef>(block2.operations[0].result);
    ASSERT_EQ(blockref2.arguments[0], resultRef);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 1);
    ASSERT_EQ(block3.predecessors[1], 2);
    ASSERT_EQ(block3.arguments.size(), 1);
    ASSERT_EQ(block3.operations[0].op, Trace::CONST);
    ASSERT_EQ(block3.operations[1].op, Trace::ADD);
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

TEST_F(InterpreterTest, emptyLoopTest) {
    auto execution = Trace::traceFunction([]() {
        emptyLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution << std::endl;
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::CONST);
    ASSERT_EQ(block0.operations[2].op, Trace::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Trace::CONST);
    ASSERT_EQ(block1.operations[1].op, Trace::ADD);
    ASSERT_EQ(block1.operations[2].op, Trace::JMP);
    auto blockref = std::get<Trace::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 2);
    ASSERT_EQ(blockref.arguments[1], std::get<Trace::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Trace::CONST);
    ASSERT_EQ(block2.operations[1].op, Trace::SUB);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, Trace::CONST);
    ASSERT_EQ(block3.operations[1].op, Trace::LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, Trace::CMP);
}

void longEmptyLoop() {
    Value iw = Value(1);
    Value iw2 = Value(2);
    for (auto start = iw; start < 20000; start = start + 1) {
        //std::cout << "loop" << std::endl;
    }
    auto iw3 = iw2 - 5;
}

TEST_F(InterpreterTest, longEmptyLoopTest) {
    auto execution = Trace::traceFunction([]() {
        longEmptyLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::CONST);
    ASSERT_EQ(block0.operations[2].op, Trace::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Trace::CONST);
    ASSERT_EQ(block1.operations[1].op, Trace::ADD);
    ASSERT_EQ(block1.operations[2].op, Trace::JMP);
    auto blockref = std::get<Trace::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 2);
    ASSERT_EQ(blockref.arguments[1], std::get<Trace::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Trace::CONST);
    ASSERT_EQ(block2.operations[1].op, Trace::SUB);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, Trace::CONST);
    ASSERT_EQ(block3.operations[1].op, Trace::LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, Trace::CMP);
}

void sumLoop() {
    Value agg = Value(1);
    for (auto start = Value(0); start < 10; start = start + 1) {
        agg = agg + 1;
    }
    auto res = agg == 10;
}

TEST_F(InterpreterTest, sumLoopTest) {

    auto execution = Trace::traceFunction([]() {
        sumLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::CONST);
    ASSERT_EQ(block0.operations[2].op, Trace::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Trace::CONST);
    ASSERT_EQ(block1.operations[1].op, Trace::ADD);
    ASSERT_EQ(block1.operations[2].op, Trace::CONST);
    ASSERT_EQ(block1.operations[3].op, Trace::ADD);
    ASSERT_EQ(block1.operations[4].op, Trace::JMP);
    auto blockref = std::get<Trace::BlockRef>(block1.operations[4].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 2);
    ASSERT_EQ(blockref.arguments[1], std::get<Trace::ValueRef>(block1.operations[3].result));
    ASSERT_EQ(blockref.arguments[0], std::get<Trace::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Trace::CONST);
    ASSERT_EQ(block2.operations[1].op, Trace::EQUALS);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, Trace::CONST);
    ASSERT_EQ(block3.operations[1].op, Trace::LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, Trace::CMP);
}

void sumWhileLoop() {
    Value agg = Value(1);
    while (agg < 20) {
        agg = agg + 1;
    }
    auto res = agg == 10;
}

TEST_F(InterpreterTest, sumWhileLoopTest) {
    auto execution = Trace::traceFunction([]() {
        sumWhileLoop();
    });
    std::cout << execution << std::endl;
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Trace::CONST);
    ASSERT_EQ(block1.operations[1].op, Trace::ADD);
    ASSERT_EQ(block1.operations[2].op, Trace::JMP);
    auto blockref = std::get<Trace::BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 1);
    ASSERT_EQ(blockref.arguments[0], std::get<Trace::ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, Trace::CONST);
    ASSERT_EQ(block2.operations[1].op, Trace::EQUALS);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 1);
    ASSERT_EQ(block3.operations[0].op, Trace::CONST);
    ASSERT_EQ(block3.operations[1].op, Trace::LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, Trace::CMP);
}

void invertedLoop() {
    Value i = Value(0);
    Value end = Value(300);
    do {
        // body
        i = i + 1;
    } while (i < end);
}

TEST_F(InterpreterTest, invertedLoopTest) {
    auto execution = Trace::traceFunction([]() {
        invertedLoop();
    });
    std::cout << execution << std::endl;
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, Trace::CONST);
    ASSERT_EQ(block0.operations[1].op, Trace::CONST);
    ASSERT_EQ(block0.operations[2].op, Trace::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, Trace::JMP);

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 0);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, Trace::CONST);
    ASSERT_EQ(block3.operations[1].op, Trace::ADD);
    ASSERT_EQ(block3.operations[2].op, Trace::LESS_THAN);
    ASSERT_EQ(block3.operations[3].op, Trace::CMP);
}

/*
void loadStoreValue() {
    auto   Trace::ADDressVal = Value(10);
      Trace::ADDress   Trace::ADDress =   Trace::ADDress(  Trace::ADDressVal);
    auto value =   Trace::ADDress.load();
    value = value + 10;
      Trace::ADDress.store(value);
}

TEST_F(InterpreterTest, loadStoreValueTest) {

    auto execution =  Trace::traceFunction([]() {
        loadStoreValue();
    });
    std::cout << execution << std::endl;
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << execution << std::endl;
    auto basicBlocks = execution->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << execution << std::endl;
    ASSERT_EQ(block0.operations[0].op,  Trace::CONST);
    ASSERT_EQ(block0.operations[1].op,  Trace::CONST);
    ASSERT_EQ(block0.operations[2].op,  Trace::JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op,  Trace::JMP);

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 0);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op,  Trace::CONST);
    ASSERT_EQ(block3.operations[1].op,   Trace::ADD);
    ASSERT_EQ(block3.operations[2].op,  Trace::LESS_THAN);
    ASSERT_EQ(block3.operations[3].op,  Trace::CMP);
}
*/

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext()
        : PipelineExecutionContext(
            0,
            0,
            nullptr,
            [](Runtime::TupleBuffer&, Runtime::WorkerContextRef) {

            },
            [](Runtime::TupleBuffer&) {
            },
            std::vector<Runtime::Execution::OperatorHandlerPtr>()){};
};

TEST_F(InterpreterTest, emitQueryTest) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);

    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::UINT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    Scan scan = Scan();
    auto emit = std::make_shared<Emit>(memoryLayout);
    scan.setChild(emit);

    auto buffer = bm->getBufferBlocking();
    buffer.setNumberOfTuples(2);

    auto address = std::addressof(buffer);
    auto value = (int64_t) address;
    auto memRef = Value<MemRef>(std::make_unique<MemRef>(value));
    RecordBuffer recordBuffer = RecordBuffer(memoryLayout, memRef);

    auto pctx = MockedPipelineExecutionContext();
    auto memRefPCTX = Value<MemRef>(std::make_unique<MemRef>((int64_t) std::addressof(pctx)));
    memRefPCTX.ref = Trace::ValueRef(INT32_MAX, 0);
    auto wctx = Runtime::WorkerContext(0, bm, 10);
    auto wctxRefPCTX = Value<MemRef>(std::make_unique<MemRef>((int64_t) std::addressof(wctx)));
    wctxRefPCTX.ref = Trace::ValueRef(INT32_MAX, 1);
    ExecutionContext executionContext = ExecutionContext(memRefPCTX, wctxRefPCTX);

    auto execution = Trace::traceFunction([&scan, &executionContext, &recordBuffer]() {
        scan.open(executionContext, recordBuffer);
        scan.close(executionContext, recordBuffer);
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution << std::endl;
}

TEST_F(InterpreterTest, selectionQueryTest) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);

    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::UINT64);
    schema->addField("f2", BasicType::UINT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    Scan scan = Scan();
    auto readFieldF1 = std::make_shared<ReadFieldExpression>(0);
    auto readFieldF2 = std::make_shared<ReadFieldExpression>(0);
    auto equalsExpression = std::make_shared<EqualsExpression>(readFieldF1, readFieldF2);
    auto selection = std::make_shared<Selection>(equalsExpression);
    scan.setChild(selection);

    auto emit = std::make_shared<Emit>(memoryLayout);
    selection->setChild(emit);

    auto buffer = bm->getBufferBlocking();
    buffer.setNumberOfTuples(10);

    auto address = std::addressof(buffer);
    auto value = (int64_t) address;
    auto memRef = Value<MemRef>(std::make_unique<MemRef>(value));
    memRef.ref = Trace::ValueRef(INT32_MAX, 0);
    RecordBuffer recordBuffer = RecordBuffer(memoryLayout, memRef);

    auto pctx = MockedPipelineExecutionContext();
    auto memRefPCTX = Value<MemRef>(std::make_unique<MemRef>((int64_t) std::addressof(pctx)));
    memRefPCTX.ref = Trace::ValueRef(INT32_MAX, 1);
    auto wctx = Runtime::WorkerContext(0, bm, 10);
    auto wctxRefPCTX = Value<MemRef>(std::make_unique<MemRef>((int64_t) std::addressof(wctx)));
    wctxRefPCTX.ref = Trace::ValueRef(INT32_MAX, 2);
    ExecutionContext executionContext = ExecutionContext(memRefPCTX, wctxRefPCTX);

    auto execution = Trace::traceFunction([&scan, &executionContext, &recordBuffer]() {
        scan.open(executionContext, recordBuffer);
        scan.close(executionContext, recordBuffer);
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution << std::endl;
}
}// namespace NES::ExecutionEngine::Experimental::Interpreter