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

#include "Experimental/NESIR/BasicBlocks/BasicBlock.hpp"
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
        NES::Logger::setupLogging("MLIRGeneratorIfTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MLIRGeneratorIfTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Tear down MLIRGeneratorIfTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down MLIRGeneratorIfTest test class."); }
};


namespace ExecutionEngine::Experimental::IR {
namespace Operations {

std::shared_ptr<ProxyCallOperation> getProxyCallOperation(ProxyCallOperation::ProxyCallType proxyCallType, bool getInputTB) {
    std::vector<std::string> getInputDataBufArgs{"inputTupleBuffer"};
    std::vector<std::string> getOutputDataBufArgs{"outputTupleBuffer"};
    std::vector<Operation::BasicType> proxyDataBufferReturnArgs{Operation::BasicType::INT8PTR};
    if (proxyCallType == ProxyCallOperation::GetDataBuffer) {
        if (getInputTB) {
            return std::make_shared<ProxyCallOperation>(Operation::GetDataBuffer,
                                                   "getInDataBufOp",
                                                   getInputDataBufArgs,
                                                   proxyDataBufferReturnArgs,
                                                   Operation::BasicType::INT8PTR);
        } else {
            return std::make_shared<ProxyCallOperation>(Operation::GetDataBuffer,
                                                   "getOutDataBufOp",
                                                   getOutputDataBufArgs,
                                                   proxyDataBufferReturnArgs,
                                                   Operation::BasicType::INT8PTR);
        }
    } else {
        return std::make_shared<ProxyCallOperation>(Operation::GetNumTuples,
                                               "getNumTuplesOp",
                                               getInputDataBufArgs,
                                               proxyDataBufferReturnArgs,
                                               Operation::BasicType::INT64);
    }
}

template<typename... Args>
BasicBlockPtr createBB(std::string identifier, int level, std::vector<Operation::BasicType> argTypes, Args... args) {
    std::vector<std::string> argList({args...});
    return std::make_shared<BasicBlock>(identifier, level, std::vector<OperationPtr>{}, argList, argTypes);
}

BasicBlockPtr
saveBB(BasicBlockPtr basicBlock, std::unordered_map<std::string, BasicBlockPtr>& savedBBs, const std::string& basicBlockName) {
    savedBBs.emplace(std::pair{basicBlockName, basicBlock});
    return savedBBs[basicBlockName];
}

/**
 * @brief Here, we dump the string representation for a simple NESIR containing an AddOperation.
 */
TEST_F(MLIRGeneratorIfTest, DISABLED_printSimpleNESIRwithAddOperation) {

    std::vector<ExecutionEngine::Experimental::IR::Operations::Operation::BasicType> executeArgTypes{};
    std::vector<std::string> executeArgNames{"InputTupleBuffer", "OutputTupleBuffer"};
    // std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<ExecutionEngine::Experimental::IR::NESIR>();


    nesIR->addRootOperation(std::make_shared<ExecutionEngine::Experimental::IR::Operations::FunctionOperation>("execute", executeArgTypes, executeArgNames, ExecutionEngine::Experimental::IR::Operations::Operation::INT64))
        ->addFunctionBasicBlock(
            std::make_shared<ExecutionEngine::Experimental::IR::BasicBlock>("executeBodyBB", 0, std::vector<ExecutionEngine::Experimental::IR::Operations::OperationPtr>{}, executeArgNames, executeArgTypes)
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

/**
 * @brief Here, we dump the string representation for a simple NESIR containing an AddOperation.
 */
TEST_F(MLIRGeneratorIfTest, printNESIRwithIFOperation) {

    std::vector<ExecutionEngine::Experimental::IR::Operations::Operation::BasicType> executeArgTypes{};
    std::vector<std::string> executeArgNames{"InputTupleBuffer", "OutputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<ExecutionEngine::Experimental::IR::NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Operation::INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {}, "inputTupleBuffer", "outputTupleBuffer")
                ->addOperation(std::make_shared<ConstantIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstantIntOperation>("const1Op", 1, 64))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
                ->addOperation(getProxyCallOperation(Operation::GetNumTuples, false))
                ->addOperation(std::make_shared<LoopOperation>(LoopOperation::ForLoop))
                ->addLoopHeadBlock(
                    saveBB(createBB("loopHeadBB", 1, {}, "iOp"), savedBBs, "loopHeadBB")
                        ->addOperation(std::make_shared<CompareOperation>("loopHeadCompareOp",
                                                                          "iOp",
                                                                          "getNumTuplesOp",
                                                                          CompareOperation::ISLT))
                        ->addOperation(std::make_shared<IfOperation>("loopHeadCompareOp",
                                                                std::vector<std::string>{"iOp"},
                                                                std::vector<std::string>{"iOp"}))
                        ->addThenBlock(
                            createBB("loopBodyBB", 2, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("inputAddressOp",
                                                                             Operation::BasicType::INT64,
                                                                             9,
                                                                             1,
                                                                             "iOp",
                                                                             "getInDataBufOp"))
                                ->addOperation(std::make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                                ->addOperation(std::make_shared<ConstantIntOperation>("const42Op", 42, 64))
                                ->addOperation(std::make_shared<AddIntOperation>("loopBodyAddOp", "loadValOp", "const42Op"))
                                ->addOperation(std::make_shared<ConstantIntOperation>("const46Op", 46, 64))
                                ->addOperation(std::make_shared<CompareOperation>("loopBodyIfCompareOp",
                                                                             "loopBodyAddOp",
                                                                             "const46Op",
                                                                             CompareOperation::ISLT))
                                ->addOperation(std::make_shared<IfOperation>("loopBodyIfCompareOp",
                                                                        std::vector<std::string>{"iOp", "loopBodyAddOp"},
                                                                        std::vector<std::string>{}))
                                ->addThenBlock(
                                    createBB("thenBB", 3, {}, "iOp", "loopBodyAddOp")
                                        ->addOperation(std::make_shared<AddressOperation>("outputAddressOp",
                                                                                     Operation::BasicType::INT64,
                                                                                     8,
                                                                                     0,
                                                                                     "iOp",
                                                                                     "getOutDataBufOp"))
                                        ->addOperation(std::make_shared<StoreOperation>("loopBodyAddOp", "outputAddressOp"))
                                        ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                                        ->addNextBlock(
                                            saveBB(createBB("loopEndBB", 2, {Operation::INT64}, "iOp"), savedBBs, "loopEndBB")
                                                ->addOperation(std::make_shared<AddIntOperation>("loopEndIncIOp", "iOp", "const1Op"))
                                                ->addOperation(
                                                    std::make_shared<BranchOperation>(std::vector<std::string>{"loopEndIncIOp"}))
                                                ->addNextBlock(savedBBs["loopHeadBB"]))))
                        ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(std::make_shared<ReturnOperation>(0)))));

    std::string resultString = R"result(NESIR {
    FunctionOperation(InputTupleBuffer, OutputTupleBuffer)

    executeBodyBB(InputTupleBuffer, OutputTupleBuffer):
        ConstantInt64Operation_int1(5)
        ConstantInt64Operation_int2(4)
        AddIntOperation_add(int1, int2)
        ReturnOperation(0)
})result";
    NES_DEBUG("\n\n" << nesIR->toString());
    // assert(resultString == nesIR->toString());
}
}// namespace NES::ExecutionEngine::Experimental::IR::Operations;
}// namespace NES::ExecutionEngine::Experimental::IR;
}// namespace NES
