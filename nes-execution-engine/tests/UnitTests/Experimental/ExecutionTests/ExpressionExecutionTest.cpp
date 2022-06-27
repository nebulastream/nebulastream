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
#include <Experimental/Interpreter/DataValue/Integer.hpp>
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
 /**
 * @brief This test tests execution of scala expression
 */
class ExpressionExecutionTest : public testing::Test {
  public:
    Trace::SSACreationPhase ssaCreationPhase;
    Trace::TraceToIRConversionPhase irCreationPhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ExpressionExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ExpressionExecutionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down ExpressionExecutionTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ExpressionExecutionTest test class." << std::endl; }


};

Value<> addExpression() {
    auto iw = Value<>(1);
    auto iw2 = Value<>(2);
    iw = iw2 + iw;
    return iw;
}

TEST_F(ExpressionExecutionTest, assignmentOperatorTest) {
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return addExpression();
    });

    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir.get() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir);
    assert(loadedModuleSuccess == 0);
    auto engine = mlirUtility->prepareEngine();
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 3);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter