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

#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <Experimental/NESIR/Operations/AddressOperation.hpp>
#include <Experimental/NESIR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Experimental/NESIR/Operations/ConstIntOperation.hpp>
#include <Experimental/NESIR/Operations/FunctionOperation.hpp>
#include <Experimental/NESIR/Operations/IfOperation.hpp>
#include <Experimental/NESIR/Operations/LoadOperation.hpp>
#include <Experimental/NESIR/Operations/LoopOperation.hpp>
#include <Experimental/NESIR/Operations/Operation.hpp>
#include <Experimental/NESIR/Operations/ProxyCallOperation.hpp>
#include <Experimental/NESIR/Operations/ReturnOperation.hpp>
#include <Experimental/NESIR/Operations/StoreOperation.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES {

class MLIRGeneratorArithmeticOpsTest : public testing::Test {
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
    std::vector<PrimitiveStamp> proxyDataBufferReturnArgs{PrimitiveStamp::INT8PTR};
    if (proxyCallType == ProxyCallOperation::GetDataBuffer) {
        if (getInputTB) {
            return std::make_shared<ProxyCallOperation>(Operation::GetDataBuffer,
                                                        "getInDataBufOp",
                                                        getInputDataBufArgs,
                                                        proxyDataBufferReturnArgs,
                                                        PrimitiveStamp::INT8PTR);
        } else {
            return std::make_shared<ProxyCallOperation>(Operation::GetDataBuffer,
                                                        "getOutDataBufOp",
                                                        getOutputDataBufArgs,
                                                        proxyDataBufferReturnArgs,
                                                        PrimitiveStamp::INT8PTR);
        }
    } else {
        return std::make_shared<ProxyCallOperation>(Operation::GetNumTuples,
                                                    "getNumTuplesOp",
                                                    getInputDataBufArgs,
                                                    proxyDataBufferReturnArgs,
                                                    PrimitiveStamp::INT64);
    }
}

template<typename... Args>
BasicBlockPtr createBB(std::string identifier, int level, std::vector<PrimitiveStamp> argTypes, Args... args) {
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
TEST_F(MLIRGeneratorArithmeticOpsTest, printSimpleNESIRwithAddOperation) {

    std::vector<ExecutionEngine::Experimental::IR::Operations::PrimitiveStamp> executeArgTypes{};
    std::vector<std::string> executeArgNames{"InputTupleBuffer", "OutputTupleBuffer"};
    auto nesIR = std::make_shared<ExecutionEngine::Experimental::IR::NESIR>();

    nesIR
        ->addRootOperation(std::make_shared<ExecutionEngine::Experimental::IR::Operations::FunctionOperation>(
            "execute",
            executeArgTypes,
            executeArgNames,
            ExecutionEngine::Experimental::IR::Operations::INT64))
        ->addFunctionBasicBlock(
            std::make_shared<ExecutionEngine::Experimental::IR::BasicBlock>(
                "executeBodyBB",
                0,
                std::vector<ExecutionEngine::Experimental::IR::Operations::OperationPtr>{},
                executeArgNames,
                executeArgTypes)
                ->addOperation(std::make_shared<ExecutionEngine::Experimental::IR::Operations::ConstIntOperation>("int1", 5, 64))
                ->addOperation(std::make_shared<ExecutionEngine::Experimental::IR::Operations::ConstIntOperation>("int2", 4, 64))
                ->addOperation(
                    std::make_shared<ExecutionEngine::Experimental::IR::Operations::AddOperation>("add", "int1", "int2", INT64))
                ->addOperation(std::make_shared<ExecutionEngine::Experimental::IR::Operations::ReturnOperation>(0)));
    NES_DEBUG("\n\n" << nesIR->toString());
}

/**
 * @brief Here, we dump the string representation for a simple NESIR containing an AddOperation.
 */
TEST_F(MLIRGeneratorArithmeticOpsTest, printNESIRwithIFOperation) {

    std::vector<ExecutionEngine::Experimental::IR::Operations::PrimitiveStamp> executeArgTypes{};
    std::vector<std::string> executeArgNames{"InputTupleBuffer", "OutputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<ExecutionEngine::Experimental::IR::NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {}, "inputTupleBuffer", "outputTupleBuffer")
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
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
                                ->addOperation(
                                    std::make_shared<
                                        AddressOperation>("inputAddressOp", PrimitiveStamp::INT64, 9, 1, "iOp", "getInDataBufOp"))
                                ->addOperation(std::make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                                ->addOperation(std::make_shared<ConstIntOperation>("const42Op", 42, 64))
                                ->addOperation(std::make_shared<AddOperation>("loopBodyAddOp", "loadValOp", "const42Op", INT64))
                                ->addOperation(std::make_shared<ConstIntOperation>("const46Op", 46, 64))
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
                                                                                          PrimitiveStamp::INT64,
                                                                                          8,
                                                                                          0,
                                                                                          "iOp",
                                                                                          "getOutDataBufOp"))
                                        ->addOperation(std::make_shared<StoreOperation>("loopBodyAddOp", "outputAddressOp"))
                                        ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                                        ->addNextBlock(
                                            saveBB(createBB("loopEndBB", 2, {INT64}, "iOp"), savedBBs, "loopEndBB")
                                                ->addOperation(
                                                    std::make_shared<AddOperation>("loopEndIncIOp", "iOp", "const1Op", INT64))
                                                ->addOperation(
                                                    std::make_shared<BranchOperation>(std::vector<std::string>{"loopEndIncIOp"}))
                                                ->addNextBlock(savedBBs["loopHeadBB"]))))
                        ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(std::make_shared<ReturnOperation>(0)))));
    NES_DEBUG("\n\n" << nesIR->toString());
}

/**
 * @brief Here, we dump the string representation for a simple NESIR containing a loop and If-Else cases.
 */
TEST_F(MLIRGeneratorArithmeticOpsTest, printNESIRwithIFElseOperation) {

    std::vector<ExecutionEngine::Experimental::IR::Operations::PrimitiveStamp> executeArgTypes{};
    std::vector<std::string> executeArgNames{"InputTupleBuffer", "OutputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<ExecutionEngine::Experimental::IR::NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {}, "inputTupleBuffer", "outputTupleBuffer")
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
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
                                ->addOperation(
                                    std::make_shared<
                                        AddressOperation>("inputAddressOp", PrimitiveStamp::INT64, 9, 1, "iOp", "getInDataBufOp"))
                                ->addOperation(std::make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                                ->addOperation(std::make_shared<ConstIntOperation>("const42Op", 42, 64))
                                ->addOperation(std::make_shared<AddOperation>("loopBodyAddOp", "loadValOp", "const42Op", INT64))
                                ->addOperation(std::make_shared<ConstIntOperation>("const46Op", 46, 64))
                                ->addOperation(std::make_shared<AddressOperation>("outputAddressOp",
                                                                                  PrimitiveStamp::INT64,
                                                                                  8,
                                                                                  0,
                                                                                  "iOp",
                                                                                  "getOutDataBufOp"))
                                ->addOperation(std::make_shared<CompareOperation>("loopBodyIfCompareOp",
                                                                                  "loopBodyAddOp",
                                                                                  "const46Op",
                                                                                  CompareOperation::ISLT))
                                ->addOperation(std::make_shared<IfOperation>("loopBodyIfCompareOp",
                                                                             std::vector<std::string>{"iOp", "loopBodyAddOp"},
                                                                             std::vector<std::string>{}))
                                ->addThenBlock(
                                    createBB("thenBB", 3, {}, "iOp", "loopBodyAddOp", "outputAddressOp")
                                        ->addOperation(std::make_shared<StoreOperation>("loopBodyAddOp", "outputAddressOp"))
                                        ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                                        ->addNextBlock(
                                            saveBB(createBB("loopEndBB", 2, {INT64}, "iOp"), savedBBs, "loopEndBB")
                                                ->addOperation(
                                                    std::make_shared<AddOperation>("loopEndIncIOp", "iOp", "const1Op", INT64))
                                                ->addOperation(
                                                    std::make_shared<BranchOperation>(std::vector<std::string>{"loopEndIncIOp"}))
                                                ->addNextBlock(savedBBs["loopHeadBB"])))
                                ->addElseBlock(
                                    createBB("elseBB", 3, {}, "iOp", "loopBodyAddOp", "outputAddressOp")
                                        ->addOperation(std::make_shared<ConstIntOperation>("const8Op", 8, 64))
                                        ->addOperation(
                                            std::make_shared<AddOperation>("elseAddOp", "loopBodyAddOp", "const8Op", INT64))
                                        ->addOperation(std::make_shared<StoreOperation>("elseAddOp", "outputAddressOp"))
                                        ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                                        ->addNextBlock(savedBBs["loopEndBB"])))
                        ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(std::make_shared<ReturnOperation>(0)))));
    NES_DEBUG("\n\n" << nesIR->toString());
}

/**
 * @brief Here, we dump the string representation for a simple NESIR containing a loop and If-Else cases.
 */
TEST_F(MLIRGeneratorArithmeticOpsTest, printNESIRWithIfElseFollowUp) {

    std::vector<ExecutionEngine::Experimental::IR::Operations::PrimitiveStamp> executeArgTypes{};
    std::vector<std::string> executeArgNames{"inputTupleBuffer", "outputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<ExecutionEngine::Experimental::IR::NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {}, "inputTupleBuffer", "outputTupleBuffer")
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
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
                                ->addOperation(
                                    std::make_shared<
                                        AddressOperation>("inputAddressOp", PrimitiveStamp::INT64, 9, 1, "iOp", "getInDataBufOp"))
                                ->addOperation(std::make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                                ->addOperation(std::make_shared<ConstIntOperation>("const42Op", 42, 64))
                                ->addOperation(std::make_shared<AddOperation>("loopBodyAddOp", "loadValOp", "const42Op", INT64))
                                ->addOperation(std::make_shared<ConstIntOperation>("const46Op", 46, 64))
                                ->addOperation(std::make_shared<CompareOperation>("loopBodyIfCompareOp",
                                                                                  "loopBodyAddOp",
                                                                                  "const46Op",
                                                                                  CompareOperation::ISLT))
                                ->addOperation(std::make_shared<IfOperation>("loopBodyIfCompareOp",
                                                                             std::vector<std::string>{"iOp", "loopBodyAddOp"},
                                                                             std::vector<std::string>{}))
                                ->addThenBlock(
                                    createBB("thenBB", 3, {}, "iOp", "loopBodyAddOp")
                                        ->addOperation(
                                            std::make_shared<AddOperation>("thenAddOp", "loopBodyAddOp", "const46Op", INT64))
                                        ->addOperation(
                                            std::make_shared<BranchOperation>(std::vector<std::string>{"iOp", "thenAddOp"}))
                                        ->addNextBlock(
                                            saveBB(createBB("loopEndBB", 2, {INT64, INT64}, "iOp", "ifAddResult"),
                                                   savedBBs,
                                                   "loopEndBB")
                                                ->addOperation(std::make_shared<AddressOperation>("outputAddressOp",
                                                                                                  PrimitiveStamp::INT64,
                                                                                                  8,
                                                                                                  0,
                                                                                                  "iOp",
                                                                                                  "getOutDataBufOp"))
                                                ->addOperation(std::make_shared<StoreOperation>("ifAddResult", "outputAddressOp"))
                                                ->addOperation(
                                                    std::make_shared<AddOperation>("loopEndIncIOp", "iOp", "const1Op", INT64))
                                                ->addOperation(
                                                    std::make_shared<BranchOperation>(std::vector<std::string>{"loopEndIncIOp"}))
                                                ->addNextBlock(savedBBs["loopHeadBB"])))
                                ->addElseBlock(
                                    createBB("elseBB", 3, {}, "iOp", "loopBodyAddOp")
                                        ->addOperation(std::make_shared<ConstIntOperation>("const8Op", 8, 64))
                                        ->addOperation(
                                            std::make_shared<AddOperation>("elseAddOp", "loopBodyAddOp", "const8Op", INT64))
                                        ->addOperation(
                                            std::make_shared<BranchOperation>(std::vector<std::string>{"iOp", "elseAddOp"}))
                                        ->addNextBlock(savedBBs["loopEndBB"])))
                        ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(std::make_shared<ReturnOperation>(0)))));
    NES_DEBUG("\n\n" << nesIR->toString());
}

/**
 * @brief Here, we dump the string representation for a simple NESIR containing a loop and If-Else cases.
 */
TEST_F(MLIRGeneratorArithmeticOpsTest, printNESIRWithNestedIfElse) {

    std::vector<ExecutionEngine::Experimental::IR::Operations::PrimitiveStamp> executeArgTypes{};
    std::vector<std::string> executeArgNames{"InputTupleBuffer", "OutputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<ExecutionEngine::Experimental::IR::NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {}, "inputTupleBuffer", "outputTupleBuffer")
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
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
                                ->addOperation(
                                    std::make_shared<
                                        AddressOperation>("inputAddressOp", PrimitiveStamp::INT64, 9, 1, "iOp", "getInDataBufOp"))
                                ->addOperation(std::make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                                ->addOperation(std::make_shared<ConstIntOperation>("const42Op", 42, 64))
                                ->addOperation(std::make_shared<AddOperation>("loopBodyAddOp", "loadValOp", "const42Op", INT64))
                                ->addOperation(std::make_shared<ConstIntOperation>("branchVal", 0, 64))
                                ->addOperation(std::make_shared<ConstIntOperation>("const46Op", 46, 64))
                                ->addOperation(std::make_shared<CompareOperation>("loopBodyIfCompareOp",
                                                                                  "loopBodyAddOp",
                                                                                  "const46Op",
                                                                                  CompareOperation::ISLT))
                                ->addOperation(
                                    make_shared<IfOperation>("loopBodyIfCompareOp",
                                                             std::vector<std::string>{"iOp", "loopBodyAddOp", "branchVal"},
                                                             std::vector<std::string>{"iOp", "loopBodyAddOp", "branchVal"}))
                                ->addThenBlock(
                                    createBB("thenBB", 3, {}, "iOp", "loopBodyAddOp", "branchVal")
                                        ->addOperation(
                                            std::make_shared<AddOperation>("thenAddOp", "loopBodyAddOp", "const46Op", INT64))
                                        ->addOperation(std::make_shared<ConstIntOperation>("const100Op", 100, 64))
                                        ->addOperation(std::make_shared<CompareOperation>("nestedIfCompare",
                                                                                          "thenAddOp",
                                                                                          "const100Op",
                                                                                          CompareOperation::ISLT))
                                        ->addOperation(
                                            make_shared<IfOperation>("nestedIfCompare",
                                                                     std::vector<std::string>{"iOp", "thenAddOp", "branchVal"},
                                                                     std::vector<std::string>{"iOp", "thenAddOp", "branchVal"}))
                                        ->addThenBlock(
                                            createBB("thenThenBB", 4, {}, "iOp", "thenAddOp", "branchVal")
                                                ->addOperation(std::make_shared<ConstIntOperation>("thenThenBranchVal", 1, 64))
                                                ->addOperation(std::make_shared<BranchOperation>(
                                                    std::vector<std::string>{"iOp", "thenAddOp", "thenThenBranchVal"}))
                                                ->addNextBlock(
                                                    saveBB(createBB("loopEndBB",
                                                                    2,
                                                                    {INT64, INT64, INT64},
                                                                    "iOp",
                                                                    "ifAddResult",
                                                                    "ifBranchVal"),
                                                           savedBBs,
                                                           "loopEndBB")
                                                        ->addOperation(std::make_shared<AddOperation>("loopEndAddResult",
                                                                                                      "ifAddResult",
                                                                                                      "ifBranchVal",
                                                                                                      INT64))
                                                        ->addOperation(std::make_shared<AddressOperation>("outputAddressOp",
                                                                                                          PrimitiveStamp::INT64,
                                                                                                          8,
                                                                                                          0,
                                                                                                          "iOp",
                                                                                                          "getOutDataBufOp"))
                                                        ->addOperation(std::make_shared<StoreOperation>("loopEndAddResult",
                                                                                                        "outputAddressOp"))
                                                        ->addOperation(std::make_shared<AddOperation>("loopEndIncIOp",
                                                                                                      "iOp",
                                                                                                      "const1Op",
                                                                                                      INT64))
                                                        ->addOperation(std::make_shared<BranchOperation>(
                                                            std::vector<std::string>{"loopEndIncIOp"}))
                                                        ->addNextBlock(savedBBs["loopHeadBB"])))
                                        ->addElseBlock(
                                            createBB("thenElseBB", 4, {}, "iOp", "thenAddOp", "branchVal")
                                                ->addOperation(std::make_shared<ConstIntOperation>("thenElseBranchVal", 47, 64))
                                                ->addOperation(std::make_shared<AddOperation>("thenElseAddOp",
                                                                                              "thenAddOp",
                                                                                              "thenElseBranchVal",
                                                                                              INT64))
                                                ->addOperation(std::make_shared<BranchOperation>(
                                                    std::vector<std::string>{"iOp", "thenElseAddOp", "thenElseBranchVal"}))
                                                ->addNextBlock(savedBBs["loopEndBB"])))
                                ->addElseBlock(
                                    createBB("elseBB", 3, {}, "iOp", "loopBodyAddOp", "branchVal")
                                        ->addOperation(std::make_shared<ConstIntOperation>("const8Op", 8, 64))
                                        ->addOperation(
                                            std::make_shared<AddOperation>("elseAddOp", "loopBodyAddOp", "const8Op", INT64))
                                        ->addOperation(std::make_shared<BranchOperation>(
                                            std::vector<std::string>{"iOp", "elseAddOp", "const8Op"}))
                                        ->addNextBlock(savedBBs["loopEndBB"])))
                        ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(std::make_shared<ReturnOperation>(0)))));
    NES_DEBUG("\n\n" << nesIR->toString());
}

/**
 * @brief Constructing an execute func, with a loop that contains an IfOperation. The if-case has a nested IfOperation.
          In addition to the nested IfOperation has simple follow up Operations after the nested IfOperation.
 */
TEST_F(MLIRGeneratorArithmeticOpsTest, printNESIRWithNestedIfElseFollowUp) {

    std::vector<ExecutionEngine::Experimental::IR::Operations::PrimitiveStamp> executeArgTypes{};
    std::vector<std::string> executeArgNames{"InputTupleBuffer", "OutputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<ExecutionEngine::Experimental::IR::NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {}, "inputTupleBuffer", "outputTupleBuffer")
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
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
                                ->addOperation(
                                    std::make_shared<
                                        AddressOperation>("inputAddressOp", PrimitiveStamp::INT64, 9, 1, "iOp", "getInDataBufOp"))
                                ->addOperation(std::make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                                ->addOperation(std::make_shared<ConstIntOperation>("const42Op", 42, 64))
                                ->addOperation(std::make_shared<AddOperation>("loopBodyAddOp", "loadValOp", "const42Op", INT64))
                                ->addOperation(std::make_shared<ConstIntOperation>("branchVal", 0, 64))
                                ->addOperation(std::make_shared<ConstIntOperation>("const46Op", 46, 64))
                                ->addOperation(std::make_shared<CompareOperation>("loopBodyIfCompareOp",
                                                                                  "loopBodyAddOp",
                                                                                  "const46Op",
                                                                                  CompareOperation::ISLT))
                                ->addOperation(
                                    make_shared<IfOperation>("loopBodyIfCompareOp",
                                                             std::vector<std::string>{"iOp", "loopBodyAddOp", "branchVal"},
                                                             std::vector<std::string>{"iOp", "loopBodyAddOp", "branchVal"}))
                                ->addThenBlock(
                                    createBB("thenBB", 3, {}, "iOp", "loopBodyAddOp", "branchVal")
                                        ->addOperation(
                                            std::make_shared<AddOperation>("thenAddOp", "loopBodyAddOp", "const46Op", INT64))
                                        ->addOperation(std::make_shared<ConstIntOperation>("const100Op", 100, 64))
                                        ->addOperation(std::make_shared<CompareOperation>("nestedIfCompare",
                                                                                          "thenAddOp",
                                                                                          "const100Op",
                                                                                          CompareOperation::ISLT))
                                        ->addOperation(
                                            make_shared<IfOperation>("nestedIfCompare",
                                                                     std::vector<std::string>{"iOp", "thenAddOp", "branchVal"},
                                                                     std::vector<std::string>{"iOp", "thenAddOp", "branchVal"}))
                                        ->addThenBlock(
                                            createBB("thenThenBB", 4, {}, "iOp", "thenAddOp", "branchVal")
                                                ->addOperation(std::make_shared<ConstIntOperation>("thenThenBranchVal", 1, 64))
                                                ->addOperation(std::make_shared<BranchOperation>(
                                                    std::vector<std::string>{"iOp", "thenAddOp", "thenThenBranchVal"}))
                                                ->addNextBlock(
                                                    saveBB(createBB("nestedIfFollowUpBB",
                                                                    3,
                                                                    {INT64, INT64, INT64},
                                                                    "iOp",
                                                                    "nestedIfAddResult",
                                                                    "ifBranchVal"),
                                                           savedBBs,
                                                           "nestedIfFollowUpBB")
                                                        ->addOperation(std::make_shared<AddOperation>("afterIfBBAddOp",
                                                                                                      "nestedIfAddResult",
                                                                                                      "ifBranchVal",
                                                                                                      INT64))
                                                        ->addOperation(std::make_shared<BranchOperation>(
                                                            std::vector<std::string>{"iOp", "afterIfBBAddOp", "ifBranchVal"}))
                                                        ->addNextBlock(
                                                            saveBB(createBB("loopEndBB",
                                                                            2,
                                                                            {INT64, INT64, INT64},
                                                                            "iOp",
                                                                            "ifAddResult",
                                                                            "ifBranchVal"),
                                                                   savedBBs,
                                                                   "loopEndBB")
                                                                ->addOperation(std::make_shared<AddOperation>("loopEndAddResult",
                                                                                                              "ifAddResult",
                                                                                                              "ifBranchVal",
                                                                                                              INT64))
                                                                ->addOperation(
                                                                    std::make_shared<AddressOperation>("outputAddressOp",
                                                                                                       PrimitiveStamp::INT64,
                                                                                                       8,
                                                                                                       0,
                                                                                                       "iOp",
                                                                                                       "getOutDataBufOp"))
                                                                ->addOperation(
                                                                    std::make_shared<StoreOperation>("loopEndAddResult",
                                                                                                     "outputAddressOp"))
                                                                ->addOperation(std::make_shared<AddOperation>("loopEndIncIOp",
                                                                                                              "iOp",
                                                                                                              "const1Op",
                                                                                                              INT64))
                                                                ->addOperation(std::make_shared<BranchOperation>(
                                                                    std::vector<std::string>{"loopEndIncIOp"}))
                                                                ->addNextBlock(savedBBs["loopHeadBB"]))))
                                        ->addElseBlock(
                                            createBB("thenElseBB", 4, {}, "iOp", "thenAddOp", "branchVal")
                                                ->addOperation(std::make_shared<ConstIntOperation>("thenElseBranchVal", 47, 64))
                                                ->addOperation(std::make_shared<AddOperation>("thenElseAddOp",
                                                                                              "thenAddOp",
                                                                                              "thenElseBranchVal",
                                                                                              INT64))
                                                ->addOperation(std::make_shared<BranchOperation>(
                                                    std::vector<std::string>{"iOp", "thenElseAddOp", "thenElseBranchVal"}))
                                                ->addNextBlock(savedBBs["nestedIfFollowUpBB"])))
                                ->addElseBlock(
                                    createBB("elseBB", 3, {}, "iOp", "loopBodyAddOp", "branchVal")
                                        ->addOperation(std::make_shared<ConstIntOperation>("const8Op", 8, 64))
                                        ->addOperation(
                                            std::make_shared<AddOperation>("elseAddOp", "loopBodyAddOp", "const8Op", INT64))
                                        ->addOperation(std::make_shared<BranchOperation>(
                                            std::vector<std::string>{"iOp", "elseAddOp", "const8Op"}))
                                        ->addNextBlock(savedBBs["loopEndBB"])))
                        ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(std::make_shared<ReturnOperation>(0)))));
    NES_DEBUG("\n\n" << nesIR->toString());
}

/**
 * @brief Constructing an execute func, with a loop that contains an IfOperation. The if-case has a nested IfOperation.
          In addition to the nested IfOperation, the if-case also contains a nested LoopOperation.
 */
TEST_F(MLIRGeneratorArithmeticOpsTest, printNESIRWithNestedIfElseAndLoopFollowUp) {

    std::vector<ExecutionEngine::Experimental::IR::Operations::PrimitiveStamp> executeArgTypes{};
    std::vector<std::string> executeArgNames{"InputTupleBuffer", "OutputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<ExecutionEngine::Experimental::IR::NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {}, "inputTupleBuffer", "outputTupleBuffer")
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
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
                                ->addOperation(
                                    std::make_shared<
                                        AddressOperation>("inputAddressOp", PrimitiveStamp::INT64, 9, 1, "iOp", "getInDataBufOp"))
                                ->addOperation(std::make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                                ->addOperation(std::make_shared<ConstIntOperation>("const42Op", 42, 64))
                                ->addOperation(std::make_shared<AddOperation>("loopBodyAddOp", "loadValOp", "const42Op", INT64))
                                ->addOperation(std::make_shared<ConstIntOperation>("branchVal", 0, 64))
                                ->addOperation(std::make_shared<ConstIntOperation>("const46Op", 46, 64))
                                ->addOperation(std::make_shared<CompareOperation>("loopBodyIfCompareOp",
                                                                                  "loopBodyAddOp",
                                                                                  "const46Op",
                                                                                  CompareOperation::ISLT))
                                ->addOperation(
                                    make_shared<IfOperation>("loopBodyIfCompareOp",
                                                             std::vector<std::string>{"iOp", "loopBodyAddOp", "branchVal"},
                                                             std::vector<std::string>{"iOp", "loopBodyAddOp", "branchVal"}))
                                ->addThenBlock(
                                    createBB("thenBB", 3, {}, "iOp", "loopBodyAddOp", "branchVal")
                                        ->addOperation(
                                            std::make_shared<AddOperation>("thenAddOp", "loopBodyAddOp", "const46Op", INT64))
                                        ->addOperation(std::make_shared<ConstIntOperation>("const100Op", 100, 64))
                                        ->addOperation(std::make_shared<ConstIntOperation>("jOp", 0, 64))
                                        ->addOperation(std::make_shared<CompareOperation>("nestedIfCompare",
                                                                                          "thenAddOp",
                                                                                          "const100Op",
                                                                                          CompareOperation::ISLT))
                                        ->addOperation(std::make_shared<IfOperation>(
                                            "nestedIfCompare",
                                            std::vector<std::string>{"iOp", "jOp", "thenAddOp", "branchVal"},
                                            std::vector<std::string>{"iOp", "jOp", "thenAddOp", "branchVal"}))
                                        ->addThenBlock(
                                            createBB("thenThenBB", 4, {}, "iOp", "jOp", "thenAddOp", "branchVal")
                                                ->addOperation(std::make_shared<ConstIntOperation>("thenThenBranchVal", 1, 64))
                                                ->addOperation(std::make_shared<BranchOperation>(
                                                    std::vector<std::string>{"iOp", "jOp", "thenAddOp", "thenThenBranchVal"}))
                                                ->addNextBlock(
                                                    saveBB(createBB("nestedIfFollowUpBB",
                                                                    3,
                                                                    {INT64, INT64, INT64, INT64},
                                                                    "iOp",
                                                                    "jOp",
                                                                    "nestedIfAddResult",
                                                                    "ifBranchVal"),
                                                           savedBBs,
                                                           "nestedIfFollowUpBB")
                                                        ->addOperation(std::make_shared<AddOperation>("afterIfBBAddOp",
                                                                                                      "nestedIfAddResult",
                                                                                                      "ifBranchVal",
                                                                                                      INT64))
                                                        ->addOperation(std::make_shared<LoopOperation>(
                                                            LoopOperation::ForLoop,
                                                            std::vector<
                                                                std::string>{"iOp", "jOp", "afterIfBBAddOp", "ifBranchVal"}))
                                                        ->addLoopHeadBlock(
                                                            //Nested Loop Start
                                                            saveBB(createBB("nestedLoopHeadBB",
                                                                            3,
                                                                            {},
                                                                            "iOp",
                                                                            "jOp",
                                                                            "loopAddInput",
                                                                            "nestedLoopUpperLimit"),
                                                                   savedBBs,
                                                                   "nestedLoopHeadBB")
                                                                ->addOperation(
                                                                    std::make_shared<CompareOperation>("nestedLoopHeadCompareOp",
                                                                                                       "jOp",
                                                                                                       "nestedLoopUpperLimit",
                                                                                                       CompareOperation::ISLT))
                                                                ->addOperation(std::make_shared<IfOperation>(
                                                                    "nestedLoopHeadCompareOp",
                                                                    std::vector<std::string>{"iOp",
                                                                                             "jOp",
                                                                                             "loopAddInput",
                                                                                             "nestedLoopUpperLimit"},
                                                                    std::vector<std::string>{"iOp",
                                                                                             "loopAddInput",
                                                                                             "nestedLoopUpperLimit"}))
                                                                ->addThenBlock(
                                                                    createBB("nestedLoopBodyBB", 4, {}, "jOp", "loopAddInput")
                                                                        ->addOperation(
                                                                            std::make_shared<AddOperation>("nestedLoopAddOp",
                                                                                                           "loopAddInput",
                                                                                                           "loopAddInput",
                                                                                                           INT64))
                                                                        ->addOperation(std::make_shared<ProxyCallOperation>(
                                                                            Operation::ProxyCallType::Other,
                                                                            "printValueFromMLIR",
                                                                            std::vector<std::string>{"nestedLoopAddOp"},
                                                                            std::vector<PrimitiveStamp>{},
                                                                            PrimitiveStamp::VOID))
                                                                        ->addOperation(
                                                                            std::make_shared<AddOperation>("nestedLoopEndIncJOp",
                                                                                                           "jOp",
                                                                                                           "const1Op",
                                                                                                           INT64))
                                                                        ->addOperation(std::make_shared<BranchOperation>(
                                                                            std::vector<std::string>{"iOp",
                                                                                                     "nestedLoopEndIncJOp",
                                                                                                     "loopAddInput",
                                                                                                     "nestedLoopUpperLimit"}))
                                                                        ->addNextBlock(savedBBs["nestedLoopHeadBB"]))
                                                                ->addElseBlock(
                                                                    saveBB(createBB("loopEndBB",
                                                                                    2,
                                                                                    {INT64, INT64, INT64},
                                                                                    "iOp",
                                                                                    "ifAddResult",
                                                                                    "ifBranchVal"),
                                                                           savedBBs,
                                                                           "loopEndBB")
                                                                        ->addOperation(
                                                                            std::make_shared<AddOperation>("loopEndAddResult",
                                                                                                           "ifAddResult",
                                                                                                           "ifBranchVal",
                                                                                                           INT64))
                                                                        ->addOperation(std::make_shared<AddressOperation>(
                                                                            "outputAddressOp",
                                                                            PrimitiveStamp::INT64,
                                                                            8,
                                                                            0,
                                                                            "iOp",
                                                                            "getOutDataBufOp"))
                                                                        ->addOperation(
                                                                            std::make_shared<StoreOperation>("loopEndAddResult",
                                                                                                             "outputAddressOp"))
                                                                        ->addOperation(
                                                                            std::make_shared<AddOperation>("loopEndIncIOp",
                                                                                                           "iOp",
                                                                                                           "const1Op",
                                                                                                           INT64))
                                                                        ->addOperation(std::make_shared<BranchOperation>(
                                                                            std::vector<std::string>{"loopEndIncIOp"}))
                                                                        ->addNextBlock(savedBBs["loopHeadBB"])))))
                                        ->addElseBlock(
                                            createBB("thenElseBB", 4, {}, "iOp", "jOp", "thenAddOp", "branchVal")
                                                ->addOperation(std::make_shared<ConstIntOperation>("thenElseBranchVal", 47, 64))
                                                ->addOperation(std::make_shared<AddOperation>("thenElseAddOp",
                                                                                              "thenAddOp",
                                                                                              "thenElseBranchVal",
                                                                                              INT64))
                                                ->addOperation(std::make_shared<BranchOperation>(
                                                    std::vector<std::string>{"iOp", "jOp", "thenElseAddOp", "thenElseBranchVal"}))
                                                ->addNextBlock(savedBBs["nestedIfFollowUpBB"])))
                                ->addElseBlock(
                                    createBB("elseBB", 3, {}, "iOp", "loopBodyAddOp", "branchVal")
                                        ->addOperation(std::make_shared<ConstIntOperation>("const8Op", 8, 64))
                                        ->addOperation(
                                            std::make_shared<AddOperation>("elseAddOp", "loopBodyAddOp", "const8Op", INT64))
                                        ->addOperation(std::make_shared<BranchOperation>(
                                            std::vector<std::string>{"iOp", "elseAddOp", "const8Op"}))
                                        ->addNextBlock(savedBBs["loopEndBB"])))
                        ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(std::make_shared<ReturnOperation>(0)))));
    NES_DEBUG("\n\n" << nesIR->toString());
}

}// namespace Operations
}// namespace ExecutionEngine::Experimental::IR
}// namespace NES
