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

#include <Interpreter/DataValue/Address.hpp>
#include <Interpreter/DataValue/Integer.hpp>
#include <Interpreter/DataValue/Value.hpp>
#include <Interpreter/Expressions/EqualsExpression.hpp>
#include <Interpreter/Expressions/ReadFieldExpression.hpp>
#include <Interpreter/FunctionCall.hpp>
#include <Interpreter/Operations/AddOp.hpp>
#include <Interpreter/Operators/Scan.hpp>
#include <Interpreter/Operators/Selection.hpp>
#include <Interpreter/SSACreationPhase.hpp>
#include <Interpreter/Trace/TraceContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <execinfo.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Interpreter {

class InterpreterTest : public testing::Test {
  public:
    SSACreationPhase ssaCreationPhase;
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

uint64_t add(uint64_t x, uint64_t y) { return x + y; }
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
    auto res = FunctionCall(add, (uint64_t) 10, (uint64_t) 11);
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
    auto executionTrace = traceFunction([]() {
        assignmentOperator();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto basicBlocks = executionTrace->getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, ADD);
    ASSERT_EQ(block0.operations[3].op, RETURN);
}

void arithmeticExpression() {
    Value iw = Value(1);
    Value iw2 = Value(2);
    Value iw3 = Value(3);
    auto result = iw - iw3 + 2 * iw2 / iw;
}

TEST_F(InterpreterTest, arithmeticExpressionTest) {
    auto executionTrace = traceFunction([]() {
        arithmeticExpression();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto basicBlocks = executionTrace->getBlocks();

    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];

    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, CONST);

    ASSERT_EQ(block0.operations[3].op, SUB);
    ASSERT_EQ(block0.operations[4].op, CONST);
    ASSERT_EQ(block0.operations[5].op, MUL);
    ASSERT_EQ(block0.operations[6].op, DIV);
    ASSERT_EQ(block0.operations[7].op, ADD);
}
/*
void logicalExpressionLessThan(TraceContext* tracer) {
    Value iw = Value(1, tracer);
    auto result = iw < 2;
}

TEST_F(InterpreterTest, logicalExpressionLessThanTest) {
    auto executionTrace = traceFunction(logicalExpressionLessThan);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, LESS_THAN);
}

void logicalExpressionEquals(TraceContext* tracer) {
    Value iw = Value(1, tracer);
    auto result = iw == 2;
}

TEST_F(InterpreterTest, logicalExpressionEqualsTest) {
    auto executionTrace = traceFunction([](auto* tc) {
        logicalExpressionEquals(tc);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, EQUALS);
}

void logicalExpressionLessEquals(TraceContext* tracer) {
    Value iw = Value(1, tracer);
    auto result = iw <= 2;
}

TEST_F(InterpreterTest, logicalExpressionLessEqualsTest) {
    auto executionTrace = traceFunction(logicalExpressionLessEquals);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, LESS_THAN);
    ASSERT_EQ(block0.operations[3].op, EQUALS);
    ASSERT_EQ(block0.operations[4].op, OR);
}

void logicalExpressionGreater(TraceContext* tracer) {
    Value iw = Value(1, tracer);
    auto result = iw > 2;
}

TEST_F(InterpreterTest, logicalExpressionGreaterTest) {
    auto executionTrace = traceFunction(logicalExpressionGreater);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, LESS_THAN);
    ASSERT_EQ(block0.operations[3].op, EQUALS);
    ASSERT_EQ(block0.operations[4].op, OR);
    ASSERT_EQ(block0.operations[5].op, NEGATE);
}

void logicalExpressionGreaterEquals(TraceContext* tracer) {
    Value iw = Value(1, tracer);
    auto result = iw >= 2;
}

TEST_F(InterpreterTest, logicalExpressionGreaterEqualsTest) {
    auto executionTrace = traceFunction(logicalExpressionGreaterEquals);
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, LESS_THAN);
    ASSERT_EQ(block0.operations[3].op, NEGATE);
}

void logicalExpression(TraceContext* tracer) {
    Value iw = Value(1, tracer);
    auto result = iw == 2 && iw < 1 || true;
}

TEST_F(InterpreterTest, logicalExpressionTest) {
    auto executionTrace = traceFunction([](auto* tc) {
        logicalExpression(tc);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, EQUALS);
    ASSERT_EQ(block0.operations[3].op, CONST);
    ASSERT_EQ(block0.operations[4].op, LESS_THAN);
    ASSERT_EQ(block0.operations[5].op, AND);
    ASSERT_EQ(block0.operations[6].op, CONST);
    ASSERT_EQ(block0.operations[7].op, OR);
}

void ifCondition(bool flag, TraceContext* tracer) {
    Value boolFlag = Value(flag, tracer);
    Value iw = Value(1, tracer);
    if (boolFlag) {
        iw = iw - 1;
    }
    iw + 42;
}

TEST_F(InterpreterTest, ifConditionTest) {
    auto executionTrace = traceFunction([](TraceContext* tracer) {
        ifCondition(true, tracer);
        tracer->reset();
        ifCondition(false, tracer);
    });

    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << executionTrace << std::endl;
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];

    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, CMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 0);
    ASSERT_EQ(block1.operations[0].op, CONST);
    ASSERT_EQ(block1.operations[1].op, SUB);
    ASSERT_EQ(block1.operations[2].op, JMP);
    auto blockref = std::get<BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments[0], std::get<ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 0);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, JMP);

    auto blockref2 = std::get<BlockRef>(block2.operations[0].input[0]);
    ASSERT_EQ(blockref2.block, 3);
    ASSERT_EQ(blockref2.arguments.size(), 1);
    ASSERT_EQ(blockref2.arguments[0], block2.arguments[0]);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 1);
    ASSERT_EQ(block3.predecessors[1], 2);
    ASSERT_EQ(block3.arguments.size(), 1);
    ASSERT_EQ(block3.operations[0].op, CONST);
    ASSERT_EQ(block3.operations[1].op, ADD);
}

void ifElseCondition(bool flag, TraceContext* tracer) {
    Value boolFlag = Value(flag, tracer);
    Value iw = Value(1, tracer);
    Value iw2 = Value(1, tracer);
    if (boolFlag) {
        iw = iw - 1;
    } else {
        iw = iw * iw2;
    }
    iw + 1;
    Operation result = Operation(RETURN);
    tracer->trace(result);
}


TEST_F(InterpreterTest, ifElseConditionTest) {
    auto executionTrace = traceFunction([](TraceContext* tracer) {
        ifElseCondition(true, tracer);
        tracer->reset();
        ifElseCondition(false, tracer);
    });

    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << executionTrace << std::endl;
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, CONST);
    ASSERT_EQ(block0.operations[3].op, CMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 0);
    ASSERT_EQ(block1.operations[0].op, CONST);
    ASSERT_EQ(block1.operations[1].op, SUB);
    ASSERT_EQ(block1.operations[2].op, JMP);
    auto blockref = std::get<BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments[0], std::get<ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 0);
    ASSERT_EQ(block2.arguments.size(), 2);
    ASSERT_EQ(block2.operations[0].op, MUL);
    ASSERT_EQ(block2.operations[1].op, JMP);
    auto blockref2 = std::get<BlockRef>(block2.operations[1].input[0]);
    ASSERT_EQ(blockref2.block, 3);
    ASSERT_EQ(blockref2.arguments.size(), 1);
    auto resultRef = std::get<ValueRef>(block2.operations[0].result);
    ASSERT_EQ(blockref2.arguments[0], resultRef);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 1);
    ASSERT_EQ(block3.predecessors[1], 2);
    ASSERT_EQ(block3.arguments.size(), 1);
    ASSERT_EQ(block3.operations[0].op, CONST);
    ASSERT_EQ(block3.operations[1].op, ADD);
}

void emptyLoop(TraceContext* tracer) {
    Value iw = Value(1, tracer);
    Value iw2 = Value(2, tracer);

    //auto result = t1 + t2;

    //auto res = (iw + icw2);
    for (auto start = iw; start < 2; start = start + 1) {
        std::cout << "loop" << std::endl;
        //iw2 = iw2 + 1;
    }
    auto iw3 = iw2 - 5;
}

TEST_F(InterpreterTest, emptyLoopTest) {
    auto execution = traceFunction([](auto* tc) {
        emptyLoop(tc);
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, CONST);
    ASSERT_EQ(block1.operations[1].op, ADD);
    ASSERT_EQ(block1.operations[2].op, JMP);
    auto blockref = std::get<BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 2);
    ASSERT_EQ(blockref.arguments[1], std::get<ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, CONST);
    ASSERT_EQ(block2.operations[1].op, SUB);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, CONST);
    ASSERT_EQ(block3.operations[1].op, LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, CMP);
}

void longEmptyLoop(TraceContext* tracer) {
    Value iw = Value(1, tracer);
    Value iw2 = Value(2, tracer);
    for (auto start = iw; start < 20000; start = start + 1) {
        //std::cout << "loop" << std::endl;
    }
    auto iw3 = iw2 - 5;
}

TEST_F(InterpreterTest, longEmptyLoopTest) {
    auto execution = traceFunction([](auto* tc) {
        longEmptyLoop(tc);
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, CONST);
    ASSERT_EQ(block1.operations[1].op, ADD);
    ASSERT_EQ(block1.operations[2].op, JMP);
    auto blockref = std::get<BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 2);
    ASSERT_EQ(blockref.arguments[1], std::get<ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, CONST);
    ASSERT_EQ(block2.operations[1].op, SUB);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, CONST);
    ASSERT_EQ(block3.operations[1].op, LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, CMP);
}

void sumLoop(TraceContext* tracer) {
    Value agg = Value(1, tracer);
    for (auto start = Value(0, tracer); start < 10; start = start + 1) {
        agg = agg + 1;
    }
    auto res = agg == 10;
}

TEST_F(InterpreterTest, sumLoopTest) {
    auto execution = traceFunction([](auto* tc) {
        sumLoop(tc);
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, CONST);
    ASSERT_EQ(block1.operations[1].op, ADD);
    ASSERT_EQ(block1.operations[2].op, CONST);
    ASSERT_EQ(block1.operations[3].op, ADD);
    ASSERT_EQ(block1.operations[4].op, JMP);
    auto blockref = std::get<BlockRef>(block1.operations[4].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 2);
    ASSERT_EQ(blockref.arguments[1], std::get<ValueRef>(block1.operations[3].result));
    ASSERT_EQ(blockref.arguments[0], std::get<ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, CONST);
    ASSERT_EQ(block2.operations[1].op, EQUALS);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, CONST);
    ASSERT_EQ(block3.operations[1].op, LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, CMP);
}

void sumWhileLoop(TraceContext* tracer) {
    Value agg = Value(1, tracer);
    while (agg < 20) {
        agg = agg + 1;
    }
    auto res = agg == 10;
}

TEST_F(InterpreterTest, sumWhileLoopTest) {
    auto execution = traceFunction([](auto* tc) {
        sumWhileLoop(tc);
    });
    std::cout << execution << std::endl;
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, CONST);
    ASSERT_EQ(block1.operations[1].op, ADD);
    ASSERT_EQ(block1.operations[2].op, JMP);
    auto blockref = std::get<BlockRef>(block1.operations[2].input[0]);
    ASSERT_EQ(blockref.block, 3);
    ASSERT_EQ(blockref.arguments.size(), 1);
    ASSERT_EQ(blockref.arguments[0], std::get<ValueRef>(block1.operations[1].result));

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 1);
    ASSERT_EQ(block2.operations[0].op, CONST);
    ASSERT_EQ(block2.operations[1].op, EQUALS);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 1);
    ASSERT_EQ(block3.operations[0].op, CONST);
    ASSERT_EQ(block3.operations[1].op, LESS_THAN);
    ASSERT_EQ(block3.operations[2].op, CMP);
}

void invertedLoop(TraceContext* tracer) {
    Value i = Value(0, tracer);
    Value end = Value(300, tracer);
    do {
        // body
        i = i + 1;
    } while (i < end);
}

TEST_F(InterpreterTest, invertedLoopTest) {
    auto execution = traceFunction([](auto* tc) {
        invertedLoop(tc);
    });
    std::cout << execution << std::endl;
    execution = ssaCreationPhase.apply(std::move(execution));
    auto basicBlocks = execution.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, JMP);

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 0);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, CONST);
    ASSERT_EQ(block3.operations[1].op, ADD);
    ASSERT_EQ(block3.operations[2].op, LESS_THAN);
    ASSERT_EQ(block3.operations[3].op, CMP);
}

void loadStoreValue(TraceContext* tracer) {
    auto addressVal = Value(10, tracer);
    Address address = Address(tracer, addressVal);
    auto value = address.load();
    value = value + 10;
    address.store(value);
}

TEST_F(InterpreterTest, loadStoreValueTest) {

    auto execution = traceFunction([](auto* tc) {
        loadStoreValue(tc);
    });
    std::cout << execution << std::endl;
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << execution << std::endl;
    auto basicBlocks = execution.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    std::cout << execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, JMP);

    auto block1 = basicBlocks[1];
    ASSERT_EQ(block1.predecessors[0], 3);
    ASSERT_EQ(block1.operations[0].op, JMP);

    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 3);
    ASSERT_EQ(block2.arguments.size(), 0);

    auto block3 = basicBlocks[3];
    ASSERT_EQ(block3.predecessors[0], 0);
    ASSERT_EQ(block3.predecessors[1], 1);
    ASSERT_EQ(block3.arguments.size(), 2);
    ASSERT_EQ(block3.operations[0].op, CONST);
    ASSERT_EQ(block3.operations[1].op, ADD);
    ASSERT_EQ(block3.operations[2].op, LESS_THAN);
    ASSERT_EQ(block3.operations[3].op, CMP);
}

TEST_F(InterpreterTest, selectionQueryTest) {
    TraceContext tracer;
    Scan scan = Scan();
    auto readFieldF1 = std::make_shared<ReadFieldExpression>(0);
    auto readFieldF2 = std::make_shared<ReadFieldExpression>(0);
    auto equalsExpression = std::make_shared<EqualsExpression>(readFieldF1, readFieldF2);
    auto selection = std::make_shared<Selection>(equalsExpression);
    scan.setChild(std::move(selection));

    auto execution = traceFunction([&scan](TraceContext* traceContext) {
        scan.open(*traceContext);
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << execution << std::endl;
}
*/
}// namespace NES::Interpreter