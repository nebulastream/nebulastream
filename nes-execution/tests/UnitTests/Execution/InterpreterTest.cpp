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

#include <Interpreter/DataValue/Integer.hpp>
#include <Interpreter/DataValue/Value.hpp>
#include <Interpreter/Operations/AddOp.hpp>
#include <Interpreter/Tracer.hpp>
#include <Util/Logger/Logger.hpp>
#include <execinfo.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Interpreter {

class InterpreterTest : public testing::Test {
  public:
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

void assignmentOperator(TraceContext& tracer) {
    Value iw = Value(1, &tracer);
    Value iw2 = Value(2, &tracer);
    iw = iw2 + iw;
}

TEST_F(InterpreterTest, assignmentOperatorTest) {
    auto tracer = TraceContext();
    assignmentOperator(tracer);

    auto executionTrace = tracer.getExecutionTrace();
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    auto execution = tracer.getExecutionTrace();
    std::cout << execution << std::endl;




    // ASSERT_EQ(cast<Integer>(result)->getValue(), 3);
}

void arithmeticExpression(TraceContext& tracer) {
    Value iw = Value(1, &tracer);
    Value iw2 = Value(2, &tracer);
    Value iw3 = Value(3, &tracer);
    auto result = iw - iw3 + 2 * iw2 / iw;
}

/**
 * @brief This test compiles a test CPP File
 */
TEST_F(InterpreterTest, arithmeticExpressionTest) {
    auto tracer = TraceContext();
    arithmeticExpression(tracer);

    auto executionTrace = tracer.getExecutionTrace();
    auto basicBlocks = executionTrace.getBlocks();
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

    auto execution = tracer.getExecutionTrace();
    std::cout << execution << std::endl;

    // ASSERT_EQ(cast<Integer>(result)->getValue(), 3);
}

void logicalExpressionLessThan(TraceContext& tracer) {
    Value iw = Value(1, &tracer);
    auto result = iw < 2;
}

TEST_F(InterpreterTest, logicalExpressionLessThanTest) {
    auto tracer = TraceContext();
    logicalExpressionLessThan(tracer);

    auto executionTrace = tracer.getExecutionTrace();
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];

    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, LESS_THAN);

    auto execution = tracer.getExecutionTrace();
    std::cout << execution << std::endl;
}

void logicalExpressionEquals(TraceContext& tracer) {
    Value iw = Value(1, &tracer);
    auto result = iw == 2;
}

TEST_F(InterpreterTest, logicalExpressionEqualsTest) {
    auto tracer = TraceContext();
    logicalExpressionEquals(tracer);

    auto executionTrace = tracer.getExecutionTrace();
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];

    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, EQUALS);

    auto execution = tracer.getExecutionTrace();
    std::cout << execution << std::endl;
}

void logicalExpressionLessEquals(TraceContext& tracer) {
    Value iw = Value(1, &tracer);
    auto result = iw <= 2;
}

TEST_F(InterpreterTest, logicalExpressionLessEqualsTest) {
    auto tracer = TraceContext();
    logicalExpressionLessEquals(tracer);

    auto executionTrace = tracer.getExecutionTrace();
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    auto execution = tracer.getExecutionTrace();
    std::cout << execution << std::endl;
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, LESS_THAN);
    ASSERT_EQ(block0.operations[3].op, EQUALS);
    ASSERT_EQ(block0.operations[4].op, OR);
}

void logicalExpressionGreater(TraceContext& tracer) {
    Value iw = Value(1, &tracer);
    auto result = iw > 2;
}

TEST_F(InterpreterTest, logicalExpressionGreaterTest) {
    auto tracer = TraceContext();
    logicalExpressionGreater(tracer);

    auto executionTrace = tracer.getExecutionTrace();
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

void logicalExpressionGreaterEquals(TraceContext& tracer) {
    Value iw = Value(1, &tracer);
    auto result = iw >= 2;
}

TEST_F(InterpreterTest, logicalExpressionGreaterEqualsTest) {
    auto tracer = TraceContext();
    logicalExpressionGreaterEquals(tracer);
    auto executionTrace = tracer.getExecutionTrace();
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    ASSERT_EQ(block0.operations[0].op, CONST);
    ASSERT_EQ(block0.operations[1].op, CONST);
    ASSERT_EQ(block0.operations[2].op, LESS_THAN);
    ASSERT_EQ(block0.operations[3].op, NEGATE);
}

void logicalExpression(TraceContext& tracer) {
    Value iw = Value(1, &tracer);
    auto result = iw == 2 && iw < 1 || true;
}

TEST_F(InterpreterTest, logicalExpressionTest) {
    auto tracer = TraceContext();
    logicalExpression(tracer);

    auto executionTrace = tracer.getExecutionTrace();
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 1);
    auto block0 = basicBlocks[0];
    auto execution = tracer.getExecutionTrace();
    std::cout << execution << std::endl;
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
    iw + 1;
}

/**
 * @brief This test compiles a test CPP File
 */
TEST_F(InterpreterTest, ifConditionTest) {
    auto tracer = TraceContext();
    ifCondition(true, &tracer);
    tracer.reset();
    ifCondition(false, &tracer);

    auto executionTrace = tracer.getExecutionTrace();
    auto basicBlocks = executionTrace.getBlocks();
    ASSERT_EQ(basicBlocks.size(), 4);
    auto block0 = basicBlocks[0];
    auto execution = tracer.getExecutionTrace();
    std::cout << execution << std::endl;
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
    ASSERT_EQ(blockref.arguments[0],  std::get<ValueRef>(block1.operations[1].result));


    auto block2 = basicBlocks[2];
    ASSERT_EQ(block2.predecessors[0], 0);
    ASSERT_EQ(block2.operations[0].op, JMP);
    auto blockref2 = std::get<BlockRef>(block2.operations[0].input[0]);
    ASSERT_EQ(blockref2.block, 3);
    ASSERT_EQ(blockref2.arguments.size(), 1);
}

void loop(TraceContext* tracer) {
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

TEST_F(InterpreterTest, valueTestLoop) {
    std::unique_ptr<Any> i1 = Integer::create<Integer>(1);
    std::unique_ptr<Any> i2 = Integer::create<Integer>(2);

    auto tracer = TraceContext();
    loop(&tracer);
    auto execution = tracer.getExecutionTrace();
    std::cout << execution << std::endl;

    // ASSERT_EQ(cast<Integer>(result)->getValue(), 3);
}

}// namespace NES::Interpreter