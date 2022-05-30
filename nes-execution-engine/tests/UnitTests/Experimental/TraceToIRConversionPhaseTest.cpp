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
#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Experimental/Trace/SSACreationPhase.hpp>
#include <Experimental/Trace/TraceContext.hpp>
#include <Experimental/Trace/TraceToIRConversionPhase.hpp>
#include <Experimental/Trace/ExecutionTrace.hpp>
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
class TraceToIRConversionPhaseTest : public testing::Test {
  public:
    Trace::SSACreationPhase ssaCreationPhase;
    Trace::TraceToIRConversionPhase irCreationPhase;
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

void assignmentOperator() {
    auto iw = Value<>(1);
    auto iw2 = Value<>(2);
    iw = iw2 + iw;
}

TEST_F(TraceToIRConversionPhaseTest, assignmentOperatorTest) {
    auto executionTrace = Trace::traceFunction([]() {
        assignmentOperator();
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir.get() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir);
}

void ifCondition(bool flag) {
    Value boolFlag = Value(flag);
    Value iw = Value(1);
    if (boolFlag) {
        iw = iw + 1;
    }
    iw + 42;
}

TEST_F(TraceToIRConversionPhaseTest, ifConditionTest) {
    auto executionTrace = Trace::traceFunction([]() {
        ifCondition(true);
        Trace::getThreadLocalTraceContext()->reset();
        ifCondition(false);
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir.get() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir);
}
}