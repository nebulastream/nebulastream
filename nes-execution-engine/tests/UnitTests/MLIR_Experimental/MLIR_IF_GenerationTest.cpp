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

#include <API/Query.hpp>
#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <Experimental/NESIR/Operations/AddressOperation.hpp>
#include <Experimental/NESIR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/ConstIntOperation.hpp>
#include <Experimental/NESIR/Operations/FunctionOperation.hpp>
#include <Experimental/NESIR/Operations/IfOperation.hpp>
#include <Experimental/NESIR/Operations/LoadOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Experimental/NESIR/Operations/Loop/LoopOperation.hpp>
#include <Experimental/NESIR/Operations/ProxyCallOperation.hpp>
#include <Experimental/NESIR/Operations/ReturnOperation.hpp>
#include <Experimental/NESIR/Types/StampFactory.hpp>
#include <Experimental/NESIR/Operations/StoreOperation.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <gtest/gtest.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <memory>
#include <mlir/IR/AsmState.h>
#include <mlir/Pass/PassManager.h>
#include <mlir/Transforms/DialectConversion.h>
#include <unordered_map>

using namespace std;
using namespace NES::ExecutionEngine::Experimental::MLIR;
using namespace NES::ExecutionEngine::Experimental::IR;
using namespace NES::ExecutionEngine::Experimental::IR::Operations;

namespace NES {
class MLIR_IF_GenerationTest : public testing::Test {
  public:
    NES::ExecutionEngine::Experimental::MLIR::MLIRUtility* mlirUtility;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MLIR_IF_GenerationTest.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("MLIRNESIRTest test class SetUpTestCase.");
    }

    void SetUp() override { mlirUtility = new NES::ExecutionEngine::Experimental::MLIR::MLIRUtility("", false); }
    static void TearDownTestCase() { NES_INFO("MLIR_IF_GenerationTest test class TearDownTestCase."); }
};

template<typename... Args>
BasicBlockPtr
createBB(std::string identifier, int level, std::vector<std::shared_ptr<Operations::BasicBlockArgument>> arguments) {
    return std::make_shared<BasicBlock>(identifier, level, std::vector<OperationPtr>{}, arguments);
}

/**
 * @brief Test MLIR Generation for Add Float Operation
 *
 * def (int x)
 *  int64Const1 = 42
 *  if(x == int64Const1){
 *     x = x + 10;
 *  } else{
 *     x = x;
 *  }
 *  return x
 */
TEST_F(MLIR_IF_GenerationTest, testIfEquals) {
    auto nesIR = std::make_shared<NESIR>();

    auto x = std::make_shared<BasicBlockArgument>("x", Types::StampFactory::createInt64Stamp());
    auto rootBasicBlock = createBB("executeBodyBB", 0, {x});
    auto int64Const1 = std::make_shared<ConstIntOperation>("int64Const1", 42, Types::StampFactory::createInt64Stamp());
    rootBasicBlock->addOperation(int64Const1);

    // if (x == int64Const1)
    auto compOp = std::make_shared<CompareOperation>("comp", x, int64Const1, CompareOperation::Comparator::IEQ);
    rootBasicBlock->addOperation(compOp);
    auto ifOp = std::make_shared<IfOperation>(compOp);
    rootBasicBlock->addOperation(ifOp);

    auto x3 = std::make_shared<BasicBlockArgument>("x3", Types::StampFactory::createInt64Stamp());
    auto mergeBlock = createBB("resultBody", 0, {x3});
    ifOp->setMergeBlock(mergeBlock);

    // true case
    {
        // int64Const2 = 10
        // x = x + int64Const2
        auto x1 = std::make_shared<BasicBlockArgument>("x1", Types::StampFactory::createInt64Stamp());
        auto trueBlock = createBB("trueBlock", 1, {x1});
        ifOp->getTrueBlockInvocation().setBlock(trueBlock);
        ifOp->getTrueBlockInvocation().addArgument(x);
        auto int64Const2 = std::make_shared<ConstIntOperation>("int64Const2", 10, Types::StampFactory::createInt64Stamp());
        trueBlock->addOperation(int64Const2);
        auto addResult = std::make_shared<AddOperation>("add", x1, int64Const2);
        trueBlock->addOperation(addResult);
        trueBlock->addNextBlock(mergeBlock, {addResult});
    }

    // else case
    {
        auto x2 = std::make_shared<BasicBlockArgument>("x2", Types::StampFactory::createInt64Stamp());
        auto falseBlock = createBB("falseBlock", 1, {x2});
        ifOp->getFalseBlockInvocation().setBlock(falseBlock);
        ifOp->getFalseBlockInvocation().addArgument(x);
        falseBlock->addNextBlock(mergeBlock, {x2});
    }

    // control-flow merge
    auto returnOperation = std::make_shared<ReturnOperation>(x3);
    mergeBlock->addOperation(returnOperation);

    std::vector<PrimitiveStamp> executeArgTypes = {PrimitiveStamp::INT64};
    std::vector<std::string> executeArgNames = {"x"};
    auto functionOp = std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Types::StampFactory::createInt64Stamp());
    nesIR->addRootOperation(functionOp)->addFunctionBasicBlock(rootBasicBlock);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);
    auto engine = mlirUtility->prepareEngine();
    auto function = (int (*)(int)) engine->lookup("execute").get();
    ASSERT_EQ(function(10), 10);
    ASSERT_EQ(function(42), 52);
}

/**
 * @brief Test MLIR Generation for Add Float Operation
 *
 * def (int x, int y)
 *  int64Const1 = 42
 *  if(x == int64Const1){
 *     int64Const2 = 11
 *     if(y > int64Const1){
*           x = x + y;
*       }
 *  } else{
 *     x = x;
 *  }
 *  return x
 */
TEST_F(MLIR_IF_GenerationTest, testNestedIfEquals) {
    auto nesIR = std::make_shared<NESIR>();

    auto x = std::make_shared<BasicBlockArgument>("x", Types::StampFactory::createInt64Stamp());
    auto y = std::make_shared<BasicBlockArgument>("y", Types::StampFactory::createInt64Stamp());
    auto rootBasicBlock = createBB("executeBodyBB", 0, {x, y});
    auto int64Const1 = std::make_shared<ConstIntOperation>("int64Const1", 42, Types::StampFactory::createInt64Stamp());
    rootBasicBlock->addOperation(int64Const1);

    // if (x == int64Const1)
    auto compOp = std::make_shared<CompareOperation>("comp", x, int64Const1, CompareOperation::Comparator::IEQ);
    rootBasicBlock->addOperation(compOp);
    auto ifOp = std::make_shared<IfOperation>(compOp);
    rootBasicBlock->addOperation(ifOp);

    auto x3 = std::make_shared<BasicBlockArgument>("x3", Types::StampFactory::createInt64Stamp());
    auto mergeBlock = createBB("resultBody", 0, {x3});
    ifOp->setMergeBlock(mergeBlock);

    // true case
    {
        // int64Const2 = 10
        // x = x + int64Const2
        auto x1 = std::make_shared<BasicBlockArgument>("x1", Types::StampFactory::createInt64Stamp());
        auto y1 = std::make_shared<BasicBlockArgument>("y1", Types::StampFactory::createInt64Stamp());
        auto trueBlock = createBB("trueBlock", 1, {x1, y1});
        ifOp->getTrueBlockInvocation().setBlock(trueBlock);
        ifOp->getTrueBlockInvocation().addArgument(x);
        ifOp->getTrueBlockInvocation().addArgument(y);
        auto int64Const2 = std::make_shared<ConstIntOperation>("int64Const2", 10, Types::StampFactory::createInt64Stamp());
        trueBlock->addOperation(int64Const2);
        auto compOp2 = std::make_shared<CompareOperation>("comp", y1, int64Const2, CompareOperation::Comparator::ISLT);
        trueBlock->addOperation(compOp2);
        auto ifOp2 = std::make_shared<IfOperation>(compOp2);
        trueBlock->addOperation(ifOp2);

        auto x4 = std::make_shared<BasicBlockArgument>("x4", Types::StampFactory::createInt64Stamp());
        auto mergeBlock2 = createBB("resultBody", 0, {x4});
        ifOp2->setMergeBlock(mergeBlock2);
        // true case
        {
            // int64Const2 = 10
            // x = x + int64Const2
            auto x5 = std::make_shared<BasicBlockArgument>("x5", Types::StampFactory::createInt64Stamp());
            auto trueBlock2 = createBB("trueBlock2", 1, {x5});
            ifOp2->getTrueBlockInvocation().setBlock(trueBlock2);
            ifOp2->getTrueBlockInvocation().addArgument(x1);
            auto int64Const4 = std::make_shared<ConstIntOperation>("int64Const4", 10, Types::StampFactory::createInt64Stamp());
            trueBlock2->addOperation(int64Const4);
            auto addResult2 = std::make_shared<AddOperation>("add", x5, int64Const4);
            trueBlock2->addOperation(addResult2);
            trueBlock2->addNextBlock(mergeBlock2, {addResult2});
        }

        // else case
        {
            auto x6 = std::make_shared<BasicBlockArgument>("x6", Types::StampFactory::createInt64Stamp());
            auto falseBlock2 = createBB("falseBlock", 1, {x6});
            ifOp2->getFalseBlockInvocation().setBlock(falseBlock2);
            ifOp2->getFalseBlockInvocation().addArgument(x1);
            falseBlock2->addNextBlock(mergeBlock2, {x6});
        }
        // control-flow merge
        mergeBlock2->addNextBlock(mergeBlock, {x4});
    }

    // else case
    {
        auto x2 = std::make_shared<BasicBlockArgument>("x2", Types::StampFactory::createInt64Stamp());
        auto falseBlock = createBB("falseBlock", 1, {x2});
        ifOp->getFalseBlockInvocation().setBlock(falseBlock);
        ifOp->getFalseBlockInvocation().addArgument(x);
        falseBlock->addNextBlock(mergeBlock, {x2});
    }

    // control-flow merge
    auto returnOperation = std::make_shared<ReturnOperation>(x3);
    mergeBlock->addOperation(returnOperation);

    std::vector<PrimitiveStamp> executeArgTypes = {PrimitiveStamp::INT64, PrimitiveStamp::INT64};
    std::vector<std::string> executeArgNames = {"x", "y"};
    auto functionOp = std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Types::StampFactory::createInt64Stamp());
    nesIR->addRootOperation(functionOp)->addFunctionBasicBlock(rootBasicBlock);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);
    auto engine = mlirUtility->prepareEngine();
    auto function = (int (*)(int, int)) engine->lookup("execute").get();
    ASSERT_EQ(function(10, 10), 10);
    ASSERT_EQ(function(42, 10), 42);
    ASSERT_EQ(function(42, 5), 52);
}

}// namespace NES