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

#include "Experimental/Interpreter/Expressions/LogicalExpressions/AndExpression.hpp"
#include "Util/Timer.hpp"
#include "Util/UtilityFunctions.hpp"
#include <API/Schema.hpp>
#include <Experimental/ExecutionEngine/CompilationBasedPipelineExecutionEngine.hpp>
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Interpreter/DataValue/MemRef.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Expressions/ConstantIntegerExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/LessThanExpression.hpp>
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
#include <Experimental/Runtime/RuntimeExecutionContext.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Experimental/Trace/ExecutionTrace.hpp>
#include <Experimental/Trace/Phases/SSACreationPhase.hpp>
#include <Experimental/Trace/Phases/TraceToIRConversionPhase.hpp>
#include <Experimental/Trace/TraceContext.hpp>
#include <Experimental/Utility/TPCHUtil.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <execinfo.h>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>

namespace NES::ExecutionEngine::Experimental::Interpreter {

/**
 * @brief This test tests query execution using th mlir backend
 */
class TPCHUtilTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TPCHUtilTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup TPCHUtilTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TPCHUtilTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TPCHUtilTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TPCHUtilTest test class." << std::endl; }
};

TEST_F(TPCHUtilTest, readLineItem) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto data = TPCHUtil::getLineitems("/home/pgrulich/projects/tpch-dbgen/", bm);
    ASSERT_EQ(data.second.getNumberOfTuples(), 6001215);

    TPCHUtil::storeBuffer("test.cache", data.second);
    auto buffer2 = TPCHUtil::getFileFromCache("test.cache", bm, data.first->getSchema());
    ASSERT_EQ(buffer2.second.getNumberOfTuples(), 6001215);
}

TEST_F(TPCHUtilTest, readLineItemAndCheckContent) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto data = TPCHUtil::getLineitems("/home/pgrulich/projects/tpch-dbgen/", bm, true);
    ASSERT_EQ(data.second.getNumberOfTuples(), 6001215);

    ASSERT_EQ(data.second[0][0].read<int64_t>(), 1);
    ASSERT_EQ(data.second[0][1].read<int64_t>(), 155190);
    ASSERT_EQ(data.second[0][2].read<int64_t>(), 7706);
    ASSERT_EQ(data.second[0][3].read<int64_t>(), 1);
    ASSERT_EQ(data.second[0][4].read<int64_t>(), 17);
    ASSERT_EQ(data.second[0][5].read<int64_t>(), 2116823);
    ASSERT_EQ(data.second[0][6].read<int64_t>(), 4);
    ASSERT_EQ(data.second[0][7].read<int64_t>(), 2);
    ASSERT_EQ(data.second[0][8].read<int64_t>(), (int64_t) 'N');
    ASSERT_EQ(data.second[0][9].read<int64_t>(), (int64_t) 'O');
    ASSERT_EQ(data.second[0][10].read<int64_t>(), 19960313);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter