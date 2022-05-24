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
#include <cstdint>
#include <gtest/gtest.h>

#include "API/Query.hpp"
#include "Common/DataTypes/BasicTypes.hpp"
#include "Experimental/NESIR/BasicBlocks/BasicBlock.hpp"
#include "mlir/IR/AsmState.h"
#include "mlir/Pass/PassManager.h"
#include "mlir/Transforms/DialectConversion.h"
#include "llvm/ExecutionEngine/JITSymbol.h"

#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>

#include <Experimental/NESIR/Operations/FunctionOperation.hpp>
#include <Experimental/NESIR/Operations/LoopOperation.hpp>
#include <Experimental/NESIR/Operations/AddressOperation.hpp>
#include <Experimental/NESIR/Operations/AddIntOperation.hpp>
#include <Experimental/NESIR/Operations/ConstantIntOperation.hpp>
#include <Experimental/NESIR/Operations/LoadOperation.hpp>
#include <Experimental/NESIR/Operations/StoreOperation.hpp>
#include <Experimental/NESIR/Operations/BranchOperation.hpp>

#include <Experimental/NESIR/Operations/IfOperation.hpp>

#include <Experimental/NESIR/Operations/CompareOperation.hpp>
#include <Experimental/NESIR/Operations/ReturnOperation.hpp>
#include "Experimental/NESIR/Operations/ProxyCallOperation.hpp"
#include <memory>
#include <unordered_map>

using namespace std;
namespace NES {
class MLIRNESIRTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MLIRNESIRTest.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("MLIRNESIRTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() { NES_INFO("MLIRNESIRTest test class TearDownTestCase."); }
};

void printBuffer(std::vector<Operation::BasicType> types,
                 uint64_t numTuples, int8_t *bufferPointer) {
    for(uint64_t i = 0; i < numTuples; ++i) {
        printf("------------\nTuple Nr. %lu\n------------\n", i+1);
        for(auto type : types) {
            switch(type) {
                case Operation::BasicType::INT1: {
                    printf("Value(INT1): %d \n", *bufferPointer);
                    bufferPointer += 1;
                    break;
                }
                case Operation::BasicType::INT8: {
                    printf("Value(INT8): %d \n", *bufferPointer);
                    bufferPointer += 1;
                    break;
                }
                case Operation::BasicType::INT16: {
                    int16_t *value = (int16_t*) bufferPointer;
                    printf("Value(INT16): %d \n", *value);
                    bufferPointer += 2;
                    break;
                }
                case Operation::BasicType::INT32: {
                    int32_t *value = (int32_t*) bufferPointer;
                    printf("Value(INT32): %d \n", *value);
                    bufferPointer += 4;
                    break;
                }
                case Operation::BasicType::INT64: {
                    int64_t *value = (int64_t*) bufferPointer;
                    printf("Value(INT64): %ld \n", *value);
                    bufferPointer += 8;
                    break;
                }
                case Operation::BasicType::BOOLEAN: {
                    bool *value = (bool*) bufferPointer;
                    printf("Value(BOOL): %s \n", (*value) ? "true" : "false");
                    bufferPointer += 1;
                    break;
                }
                case Operation::BasicType::CHAR: {
                    char *value = (char*) bufferPointer;
                    printf("Value(CHAR): %c \n", *value);
                    bufferPointer += 1;
                    break;
                }
                case Operation::BasicType::FLOAT: {
                    float *value = (float*) bufferPointer;
                    printf("Value(FLOAT): %f \n", *value);
                    bufferPointer += 4;
                    break;
                }
                case Operation::BasicType::DOUBLE: {
                    double *value = (double*) bufferPointer;
                    printf("Value(DOUBLE): %f \n", *value);
                    bufferPointer += 8;
                    break;
                }
                default:
                    break;
            }
        }
    }
}

shared_ptr<ProxyCallOperation> getProxyCallOperation(ProxyCallOperation::ProxyCallType proxyCallType, bool getInputTB) {
    std::vector<string> getInputDataBufArgs{"inputTupleBuffer"};
    std::vector<string> getOutputDataBufArgs{"outputTupleBuffer"};
    std::vector<Operation::BasicType> proxyDataBufferReturnArgs{Operation::BasicType::INT8PTR};
    if (proxyCallType == ProxyCallOperation::GetDataBuffer) {
        if(getInputTB) {
            return make_shared<ProxyCallOperation>(Operation::GetDataBuffer, "getInDataBufOp", getInputDataBufArgs, 
                                                        proxyDataBufferReturnArgs, Operation::BasicType::INT8PTR);
        } else {
            return make_shared<ProxyCallOperation>(Operation::GetDataBuffer, "getOutDataBufOp", getOutputDataBufArgs, 
                                                        proxyDataBufferReturnArgs, Operation::BasicType::INT8PTR);
        }
    } else {
        return make_shared<ProxyCallOperation>(Operation::GetNumTuples, "getNumTuplesOp", getInputDataBufArgs, 
                                               proxyDataBufferReturnArgs, Operation::BasicType::INT64);
    }
}

pair<NES::Runtime::TupleBuffer, NES::Runtime::TupleBuffer> createInAndOutputBuffers() {
    const uint64_t numTuples = 2;
    auto buffMgr = new NES::Runtime::BufferManager(); //shared pointer causes crash (destructor problem)

    // Create InputBuffer and fill it with data.
    auto inputBuffer = buffMgr->getBufferBlocking();
    struct __attribute__((packed)) testTupleStruct{
        int8_t a;
        int64_t b;
    };
    //Todo try with vector -> maybe could insert numTuples dynamically
    testTupleStruct testTuplesArray[numTuples] = { testTupleStruct{1, 2}, testTupleStruct{3,4} };
    auto inputBufferPointer = inputBuffer.getBuffer<testTupleStruct>();
    memcpy(inputBufferPointer, &testTuplesArray, sizeof(testTupleStruct) * numTuples);
    inputBuffer.setNumberOfTuples(numTuples);

    auto outputBuffer = buffMgr->getBufferBlocking();   
    return pair{inputBuffer, outputBuffer};
}

template<typename... Args>
BasicBlockPtr createBB(std::string identifier, int level, Args ... args) {
    std::vector<std::string> argList({args...});
    return  make_shared<BasicBlock>(identifier, std::vector<OperationPtr>{}, argList, level);
}

BasicBlockPtr saveBB(BasicBlockPtr basicBlock, std::unordered_map<std::string, BasicBlockPtr>& savedBBs, const std::string& basicBlockName) {
    savedBBs.emplace(std::pair{basicBlockName, basicBlock});
    return savedBBs[basicBlockName];
}

TEST(MLIRNESIRTEST_TYPES, NESIRSimpleTypeTest) {
    const uint64_t numTuples = 2;
    auto inAndOutputBuffer = createInAndOutputBuffers();

    //Debug Print
    auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other, "printValueFromMLIR", 
                                                        std::vector<string>{"iOp"}, std::vector<Operation::BasicType>{}, 
                                                        Operation::BasicType::VOID);

    //Setup: Execute function args - A map that allows to save and reuse defined BBs - NESIR module.
    std::vector<Operation::BasicType> executeArgTypes{Operation::INT8PTR, Operation::INT8PTR};
    std::vector<std::string> executeArgNames{"inputTupleBuffer", "outputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<NES::NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>( "execute", executeArgTypes, executeArgNames, Operation::INT64))
    ->addFunctionBasicBlock(
        createBB("executeBodyBB", 0, "inputTupleBuffer", "outputTupleBuffer")
        ->addOperation(std::make_shared<ConstantIntOperation>("iOp", 0, 64))
        ->addOperation(std::make_shared<ConstantIntOperation>("const1Op", 1, 64))
        ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
        ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
        ->addOperation(getProxyCallOperation(Operation::GetNumTuples, false))
        ->addOperation(make_shared<LoopOperation>(LoopOperation::ForLoop))
        ->addLoopHeadBlock(
            saveBB(createBB("loopHeadBB", 1, "iOp"), savedBBs, "loopHeadBB")
            ->addOperation(std::make_shared<CompareOperation>("loopHeadCompareOp", "iOp", "getNumTuplesOp", CompareOperation::ISLT))
            ->addOperation(make_shared<IfOperation>("loopHeadCompareOp", std::vector<std::string>{"iOp"}, std::vector<std::string>{"iOp"}))
            ->addThenBlock(
                createBB("loopBodyBB", 2, "iOp")
                ->addOperation(make_shared<AddressOperation>("inputAddressOp", NES::Operation::BasicType::INT64, 9, 1, "iOp", "getInDataBufOp"))
                ->addOperation(make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                ->addOperation(std::make_shared<ConstantIntOperation>("ifCompareConstOp", 20, 64))
                ->addOperation(std::make_shared<CompareOperation>("loopBodyIfCompareOp", "loadValOp", "ifCompareConstOp", CompareOperation::ISLT))
                ->addOperation(make_shared<IfOperation>("loopBodyIfCompareOp", std::vector<std::string>{"iOp", "loadValOp"}, std::vector<std::string>{"iOp", "loadValOp"}))
                ->addThenBlock(
                    createBB("thenBB", 3, "iOp", "loadValOp")
                    ->addOperation(std::make_shared<ConstantIntOperation>("thenAddConstOp", 10, 64))
                    ->addOperation(std::make_shared<AddIntOperation>("thenAddOp", "loadValOp", "thenAddConstOp"))
                    ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp", "thenAddOp"}))
                    ->addNextBlock(
                saveBB(createBB("loopEndBB", 2, "iOp", "loadValOp"), savedBBs, "loopEndBB")
                ->addOperation(make_shared<AddressOperation>("outputAddressOp", NES::Operation::BasicType::INT64, 8, 0, "iOp", "getOutDataBufOp"))
                ->addOperation(make_shared<StoreOperation>("loadValOp", "outputAddressOp"))
                ->addOperation(make_shared<AddIntOperation>("loopEndIncIOp", "iOp", "const1Op"))
                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"loopEndIncIOp"}))
                ->addNextBlock(savedBBs["loopHeadBB"])
                    )
                )
                ->addElseBlock(
                    createBB("elseBB", 3, "iOp", "loadValOp")
                    ->addOperation(std::make_shared<ConstantIntOperation>("elseAddConstOp", 30, 64))
                    ->addOperation(make_shared<AddIntOperation>("elseAddOp", "loadValOp", "elseAddConstOp"))
                    ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp", "elseAddOp"}))
                    ->addNextBlock(savedBBs["loopEndBB"])
                )
            )
        ->addElseBlock(
            createBB("executeReturnBB", 1)
            ->addOperation(make_shared<ReturnOperation>(0))
        )
        )
    );

    // NESIR to MLIR
    auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);

    mlirUtility->runJit(false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

    // Print OutputBuffer after execution.
    std::vector<Operation::BasicType> types{Operation::INT64};
    auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}


}// namespace NES