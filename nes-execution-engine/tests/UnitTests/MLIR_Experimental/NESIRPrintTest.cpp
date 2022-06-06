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

#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>


#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>

#include <Experimental/NESIR/Operations/AddIntOperation.hpp>
#include <Experimental/NESIR/Operations/AddressOperation.hpp>
#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/ConstantIntOperation.hpp>
#include <Experimental/NESIR/Operations/FunctionOperation.hpp>
#include <Experimental/NESIR/Operations/LoadOperation.hpp>
#include <Experimental/NESIR/Operations/LoopOperation.hpp>
#include <Experimental/NESIR/Operations/StoreOperation.hpp>

#include <Experimental/NESIR/Operations/IfOperation.hpp>

#include "Experimental/NESIR/Operations/Operation.hpp"
#include "Experimental/NESIR/Operations/ProxyCallOperation.hpp"
#include <Experimental/NESIR/Operations/CompareOperation.hpp>
#include <Experimental/NESIR/Operations/ReturnOperation.hpp>

#include <sstream>

namespace NES {

class MLIRGeneratorIfTest : public testing::Test {
  public:

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GlobalQueryPlanUpdatePhaseTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup GlobalQueryPlanUpdatePhaseTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Tear down GlobalQueryPlanUpdatePhaseTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down GlobalQueryPlanUpdatePhaseTest test class."); }
};


/**
 * @brief In this test we execute query merger phase on a single query plan.
 */
TEST_F(MLIRGeneratorIfTest, executeQueryMergerPhaseForSingleQueryPlan) {

    std::vector<ExecutionEngine::Experimental::IR::Operations::Operation::BasicType> executeArgTypes{};
    std::vector<std::string> executeArgNames{"InputTupleBuffer", "OutputTupleBuffer"};
    // std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<ExecutionEngine::Experimental::IR::NESIR>();


    nesIR->addRootOperation(std::make_shared<ExecutionEngine::Experimental::IR::Operations::FunctionOperation>("execute", executeArgTypes, executeArgNames, ExecutionEngine::Experimental::IR::Operations::Operation::INT64))
        ->addFunctionBasicBlock(
            make_shared<ExecutionEngine::Experimental::IR::BasicBlock>("executeBodyBB", 0, std::vector<ExecutionEngine::Experimental::IR::Operations::OperationPtr>{}, executeArgNames, executeArgTypes)
                ->addOperation(std::make_shared<ExecutionEngine::Experimental::IR::Operations::ConstantIntOperation>("int1", 5, 64))
                ->addOperation(std::make_shared<ExecutionEngine::Experimental::IR::Operations::ConstantIntOperation>("int2", 4, 64))
                ->addOperation(std::make_shared<ExecutionEngine::Experimental::IR::Operations::AddIntOperation>("add", "int1", "int2"))
                // TODO enable return of add result
                ->addOperation(std::make_shared<ExecutionEngine::Experimental::IR::Operations::ReturnOperation>(0)));

    std::string resultString = R"result(NESIR {
    FunctionOperation(InputTupleBuffer, OutputTupleBuffer)

    executeBodyBB(InputTupleBuffer, OutputTupleBuffer):
        ConstantInt64Operation_int1(5)
        ConstantInt64Operation_int2(4)
        AddIntOperation_add(int1, int2)
        ReturnOperation(0)
})result";
    NES_DEBUG("\n\n" << nesIR->toString());
    assert(resultString == nesIR->toString());
}

}// namespace NES
