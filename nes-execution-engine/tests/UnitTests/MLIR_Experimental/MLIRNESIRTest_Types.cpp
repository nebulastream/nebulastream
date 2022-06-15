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
#include <float.h>

#include <API/Query.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <mlir/IR/AsmState.h>
#include <mlir/IR/BuiltinTypes.h>
#include <mlir/Pass/PassManager.h>
#include <mlir/Transforms/DialectConversion.h>
#include <llvm/ExecutionEngine/JITSymbol.h>

#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>

#include <Experimental/NESIR/Operations/ArithmeticOperations/AddIntOperation.hpp>
#include <Experimental/NESIR/Operations/ArithmeticOperations/AddFloatOperation.hpp>
#include <Experimental/NESIR/Operations/AddressOperation.hpp>
#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/ConstIntOperation.hpp>
#include <Experimental/NESIR/Operations/ConstFloatOperation.hpp>
#include <Experimental/NESIR/Operations/FunctionOperation.hpp>
#include <Experimental/NESIR/Operations/LoadOperation.hpp>
#include <Experimental/NESIR/Operations/LoopOperation.hpp>
#include <Experimental/NESIR/Operations/StoreOperation.hpp>

#include <Experimental/NESIR/Operations/IfOperation.hpp>

#include "Experimental/NESIR/Operations/ProxyCallOperation.hpp"
#include <Experimental/NESIR/Operations/CompareOperation.hpp>
#include <Experimental/NESIR/Operations/ReturnOperation.hpp>
#include <memory>
#include <unordered_map>

using namespace std;
using namespace NES::ExecutionEngine::Experimental::MLIR;
using namespace NES::ExecutionEngine::Experimental::IR;
using namespace NES::ExecutionEngine::Experimental::IR::Operations;

namespace NES {
class MLIRNESIRTest_ArithmeticOps : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MLIRNESIRTest.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("MLIRNESIRTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() { NES_INFO("MLIRNESIRTest test class TearDownTestCase."); }
};

void printBuffer(std::vector<Operations::Operation::BasicType> types, uint64_t numTuples, int8_t* bufferPointer) {
    for (uint64_t i = 0; i < numTuples; ++i) {
        printf("------------\nTuple Nr. %lu\n------------\n", i + 1);
        for (auto type : types) {
            switch (type) {
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
                    int16_t* value = (int16_t*) bufferPointer;
                    printf("Value(INT16): %d \n", *value);
                    bufferPointer += 2;
                    break;
                }
                case Operation::BasicType::INT32: {
                    int32_t* value = (int32_t*) bufferPointer;
                    printf("Value(INT32): %d \n", *value);
                    bufferPointer += 4;
                    break;
                }
                case Operation::BasicType::INT64: {
                    int64_t* value = (int64_t*) bufferPointer;
                    printf("Value(INT64): %ld \n", *value);
                    bufferPointer += 8;
                    break;
                }
                case Operation::BasicType::UINT8: {
                    uint8_t* value = (uint8_t*) bufferPointer;
                    printf("Value(UINT8): %u \n", *value);
                    bufferPointer += 1;
                    break;
                }
                case Operation::BasicType::UINT16: {
                    uint16_t* value = (uint16_t*) bufferPointer;
                    printf("Value(UINT16): %u \n", *value);
                    bufferPointer += 2;
                    break;
                }
                case Operation::BasicType::UINT32: {
                    uint32_t* value = (uint32_t*) bufferPointer;
                    printf("Value(UINT32): %u \n", *value);
                    bufferPointer += 4;
                    break;
                }
                case Operation::BasicType::UINT64: {
                    uint64_t* value = (uint64_t*) bufferPointer;
                    printf("Value(UINT64): %lu \n", *value);
                    bufferPointer += 8;
                    break;
                }
                case Operation::BasicType::BOOLEAN: {
                    bool* value = (bool*) bufferPointer;
                    printf("Value(BOOL): %s \n", (*value) ? "true" : "false");
                    bufferPointer += 1;
                    break;
                }
                case Operation::BasicType::CHAR: {
                    char* value = (char*) bufferPointer;
                    printf("Value(CHAR): %c \n", *value);
                    bufferPointer += 1;
                    break;
                }
                case Operation::BasicType::FLOAT: {
                    float* value = (float*) bufferPointer;
                    printf("Value(FLOAT): %f \n", *value);
                    bufferPointer += 4;
                    break;
                }
                case Operation::BasicType::DOUBLE: {
                    double* value = (double*) bufferPointer;
                    printf("Value(DOUBLE): %f \n", *value);
                    bufferPointer += 8;
                    break;
                }
                default: break;
            }
        }
    }
}

shared_ptr<ProxyCallOperation> getProxyCallOperation(ProxyCallOperation::ProxyCallType proxyCallType, bool getInputTB) {
    std::vector<string> getInputDataBufArgs{"inputTupleBuffer"};
    std::vector<string> getOutputDataBufArgs{"outputTupleBuffer"};
    std::vector<Operation::BasicType> proxyDataBufferReturnArgs{Operation::BasicType::INT8PTR};
    if (proxyCallType == ProxyCallOperation::GetDataBuffer) {
        if (getInputTB) {
            return make_shared<ProxyCallOperation>(Operation::GetDataBuffer,
                                                   "getInDataBufOp",
                                                   getInputDataBufArgs,
                                                   proxyDataBufferReturnArgs,
                                                   Operation::BasicType::INT8PTR);
        } else {
            return make_shared<ProxyCallOperation>(Operation::GetDataBuffer,
                                                   "getOutDataBufOp",
                                                   getOutputDataBufArgs,
                                                   proxyDataBufferReturnArgs,
                                                   Operation::BasicType::INT8PTR);
        }
    } else {
        return make_shared<ProxyCallOperation>(Operation::GetNumTuples,
                                               "getNumTuplesOp",
                                               getInputDataBufArgs,
                                               proxyDataBufferReturnArgs,
                                               Operation::BasicType::INT64);
    }
}

pair<NES::Runtime::TupleBuffer, NES::Runtime::TupleBuffer> createInAndOutputBuffers() {
    const uint64_t numTuples = 2;
    auto buffMgr = std::make_shared<NES::Runtime::BufferManager>();

    // Create InputBuffer and fill it with data.
    auto inputBuffer = buffMgr->getBufferBlocking();
    struct __attribute__((packed)) testTupleStruct {
        int8_t a;
        int64_t b;
    };
    testTupleStruct testTuplesArray[numTuples] = {testTupleStruct{1, 2}, testTupleStruct{3, 4}};
    auto inputBufferPointer = inputBuffer.getBuffer<testTupleStruct>();
    memcpy(inputBufferPointer, &testTuplesArray, sizeof(testTupleStruct) * numTuples);
    inputBuffer.setNumberOfTuples(numTuples);

    auto outputBuffer = buffMgr->getBufferBlocking();
    return pair{inputBuffer, outputBuffer};
}

template<typename... Args>
BasicBlockPtr createBB(std::string identifier, int level, std::vector<Operation::BasicType> argTypes, Args... args) {
    std::vector<std::string> argList({args...});
    return make_shared<BasicBlock>(identifier, level, std::vector<OperationPtr>{}, argList, argTypes);
}

BasicBlockPtr
saveBB(BasicBlockPtr basicBlock, std::unordered_map<std::string, BasicBlockPtr>& savedBBs, const std::string& basicBlockName) {
    savedBBs.emplace(std::pair{basicBlockName, basicBlock});
    return savedBBs[basicBlockName];
}

TEST(MLIRNESIRTEST_TYPES, DISABLED_storeAllBasicValues) {
    const uint64_t numTuples = 1;
    auto buffMgr = std::make_shared<NES::Runtime::BufferManager>();
    auto outputBuffer = buffMgr->getBufferBlocking();

    //Debug Print
    auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other,
                                                        "printValueFromMLIR",
                                                        std::vector<string>{"iOp"},
                                                        std::vector<Operation::BasicType>{},
                                                        Operation::BasicType::VOID);

    //Setup: Execute function args - A map that allows to save and reuse defined BBs - NESIR module.
    std::vector<Operation::BasicType> executeArgTypes{Operation::INT8PTR, Operation::INT8PTR};
    std::vector<std::string> executeArgNames{"inputTupleBuffer", "outputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Operation::INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {})
                // ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
                ->addOperation(std::make_shared<ConstIntOperation>("int8",   8, /*bits*/ 8))
                ->addOperation(std::make_shared<ConstIntOperation>("int16", 16, /*bits*/ 16))
                ->addOperation(std::make_shared<ConstIntOperation>("int32", 32, /*bits*/ 32))
                ->addOperation(std::make_shared<ConstIntOperation>("int64", -64,/*bits*/ 64))
                ->addOperation(std::make_shared<ConstIntOperation>("uint8",  UINT8_MAX, /*bits*/ 8))
                ->addOperation(std::make_shared<ConstIntOperation>("uint16", UINT16_MAX, /*bits*/ 16))
                ->addOperation(std::make_shared<ConstIntOperation>("uint32", UINT32_MAX, /*bits*/ 32))
                ->addOperation(std::make_shared<ConstIntOperation>("uint64", UINT64_MAX, /*bits*/ 64))
                ->addOperation(std::make_shared<ConstFloatOperation>("float", 4.2, /*bits*/ 32))
                ->addOperation(std::make_shared<ConstFloatOperation>("double", -4.2, /*bits*/ 64))
                ->addOperation(std::make_shared<ConstIntOperation>("char", 'a', /*bits*/ 8))
                ->addOperation(std::make_shared<ConstIntOperation>("bool", false, /*bits*/ 1))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp1", Operation::INT8,  120, 0, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp2", Operation::INT16, 120, 1, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp3", Operation::INT32, 120, 3, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp4", Operation::INT64, 120, 7, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp5", Operation::UINT8, 120, 15, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp6", Operation::UINT16, 120, 16, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp7", Operation::UINT32, 120, 18, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp8", Operation::UINT64, 120, 22, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp9", Operation::FLOAT,  120, 30, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp10", Operation::DOUBLE, 120, 34, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp11", Operation::CHAR, 120, 42, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp12", Operation::BOOLEAN, 120, 43, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<StoreOperation>("int8", "outBufAddrOp1"))
                ->addOperation(std::make_shared<StoreOperation>("int16", "outBufAddrOp2"))
                ->addOperation(std::make_shared<StoreOperation>("int32", "outBufAddrOp3"))
                ->addOperation(std::make_shared<StoreOperation>("int64", "outBufAddrOp4"))
                ->addOperation(std::make_shared<StoreOperation>("uint8", "outBufAddrOp5"))
                ->addOperation(std::make_shared<StoreOperation>("uint16", "outBufAddrOp6"))
                ->addOperation(std::make_shared<StoreOperation>("uint32", "outBufAddrOp7"))
                ->addOperation(std::make_shared<StoreOperation>("uint64", "outBufAddrOp8"))
                ->addOperation(std::make_shared<StoreOperation>("float", "outBufAddrOp9"))
                ->addOperation(std::make_shared<StoreOperation>("double", "outBufAddrOp10"))
                ->addOperation(std::make_shared<StoreOperation>("char", "outBufAddrOp11"))
                ->addOperation(std::make_shared<StoreOperation>("bool", "outBufAddrOp12"))
                ->addOperation(make_shared<ReturnOperation>(0)));

    // NESIR to MLIR
    //mlir::IntegerType::get(context, constIntOp->getNumBits(), mlir::IntegerType::Signless);
    auto mlirUtility = new MLIRUtility("", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);

    mlirUtility->runJit(false, nullptr, addressof(outputBuffer));

    // Print OutputBuffer after execution.
    std::vector<Operation::BasicType> types{Operation::INT8, Operation::INT16, Operation::INT32, Operation::INT64, 
                                            Operation::UINT8, Operation::UINT16, Operation::UINT32, Operation::UINT64,
                                            Operation::FLOAT, Operation::DOUBLE, Operation::CHAR, Operation::BOOLEAN};
    auto outputBufferPointer = outputBuffer.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}

TEST(MLIRNESIRTEST_TYPES, DISABLED_loadAndStoreAllBasicValues) {
    const uint64_t numTuples = 1;
    auto buffMgr = std::make_shared<NES::Runtime::BufferManager>();
    // Create InputBuffer and fill it with data.
    auto inputBuffer = buffMgr->getBufferBlocking();
    struct __attribute__((packed)) testTupleStruct {
        int8_t a;
        int16_t b;
        int32_t c;
        int64_t d;
        uint8_t e;
        uint16_t f;
        uint32_t g;
        uint64_t h;
        float i;
        double j;
        char k;
        bool l;
    };
    testTupleStruct testTuplesArray[numTuples] = {
        testTupleStruct{8, 16, 32, 64, UINT8_MAX, UINT16_MAX, UINT32_MAX, UINT64_MAX, 4.2, -4.2, 'a', false}
    };
    auto inputBufferPointer = inputBuffer.getBuffer<testTupleStruct>();
    memcpy(inputBufferPointer, &testTuplesArray, sizeof(testTupleStruct) * numTuples);
    inputBuffer.setNumberOfTuples(numTuples);
    auto outputBuffer = buffMgr->getBufferBlocking();

    //Debug Print
    auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other,
                                                        "printValueFromMLIR",
                                                        std::vector<string>{"iOp"},
                                                        std::vector<Operation::BasicType>{},
                                                        Operation::BasicType::VOID);

    //Setup: Execute function args - A map that allows to save and reuse defined BBs - NESIR module.
    std::vector<Operation::BasicType> executeArgTypes{Operation::INT8PTR, Operation::INT8PTR};
    std::vector<std::string> executeArgNames{"inputTupleBuffer", "outputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Operation::INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {})
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
                ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp1", Operation::INT8,  44, 0, "", "getInDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp2", Operation::INT16, 44, 1, "", "getInDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp3", Operation::INT32, 44, 3, "", "getInDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp4", Operation::INT64, 44, 7, "", "getInDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp5", Operation::UINT8, 44, 15, "", "getInDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp6", Operation::UINT16, 44, 16, "", "getInDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp7", Operation::UINT32, 44, 18, "", "getInDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp8", Operation::UINT64, 44, 22, "", "getInDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp9", Operation::FLOAT,  44, 30, "", "getInDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp10", Operation::DOUBLE, 44, 34, "", "getInDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp11", Operation::CHAR, 44, 42, "", "getInDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp12", Operation::BOOLEAN, 44, 43, "", "getInDataBufOp"))
                ->addOperation(std::make_shared<LoadOperation>("int8", "inBufAddrOp1"))
                ->addOperation(std::make_shared<LoadOperation>("int16", "inBufAddrOp2"))
                ->addOperation(std::make_shared<LoadOperation>("int32", "inBufAddrOp3"))
                ->addOperation(std::make_shared<LoadOperation>("int64", "inBufAddrOp4"))
                ->addOperation(std::make_shared<LoadOperation>("uint8", "inBufAddrOp5"))
                ->addOperation(std::make_shared<LoadOperation>("uint16", "inBufAddrOp6"))
                ->addOperation(std::make_shared<LoadOperation>("uint32", "inBufAddrOp7"))
                ->addOperation(std::make_shared<LoadOperation>("uint64", "inBufAddrOp8"))
                ->addOperation(std::make_shared<LoadOperation>("float", "inBufAddrOp9"))
                ->addOperation(std::make_shared<LoadOperation>("double", "inBufAddrOp10"))
                ->addOperation(std::make_shared<LoadOperation>("char", "inBufAddrOp11"))
                ->addOperation(std::make_shared<LoadOperation>("bool", "inBufAddrOp12"))

                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp1", Operation::INT8,  44, 0, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp2", Operation::INT16, 44, 1, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp3", Operation::INT32, 44, 3, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp4", Operation::INT64, 44, 7, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp5", Operation::UINT8, 44, 15, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp6", Operation::UINT16, 44, 16, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp7", Operation::UINT32, 44, 18, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp8", Operation::UINT64, 44, 22, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp9", Operation::FLOAT,  44, 30, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp10", Operation::DOUBLE, 44, 34, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp11", Operation::CHAR, 44, 42, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp12", Operation::BOOLEAN, 44, 43, "", "getOutDataBufOp"))
                ->addOperation(std::make_shared<StoreOperation>("int8", "outBufAddrOp1"))
                ->addOperation(std::make_shared<StoreOperation>("int16", "outBufAddrOp2"))
                ->addOperation(std::make_shared<StoreOperation>("int32", "outBufAddrOp3"))
                ->addOperation(std::make_shared<StoreOperation>("int64", "outBufAddrOp4"))
                ->addOperation(std::make_shared<StoreOperation>("uint8", "outBufAddrOp5"))
                ->addOperation(std::make_shared<StoreOperation>("uint16", "outBufAddrOp6"))
                ->addOperation(std::make_shared<StoreOperation>("uint32", "outBufAddrOp7"))
                ->addOperation(std::make_shared<StoreOperation>("uint64", "outBufAddrOp8"))
                ->addOperation(std::make_shared<StoreOperation>("float", "outBufAddrOp9"))
                ->addOperation(std::make_shared<StoreOperation>("double", "outBufAddrOp10"))
                ->addOperation(std::make_shared<StoreOperation>("char", "outBufAddrOp11"))
                ->addOperation(std::make_shared<StoreOperation>("bool", "outBufAddrOp12"))
                ->addOperation(make_shared<ReturnOperation>(0)));

    // NESIR to MLIR
    //mlir::IntegerType::get(context, constIntOp->getNumBits(), mlir::IntegerType::Signless);
    auto mlirUtility = new MLIRUtility("", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);

    mlirUtility->runJit(false, addressof(inputBuffer), addressof(outputBuffer));

    // Print OutputBuffer after execution.
    std::vector<Operation::BasicType> types{Operation::INT8, Operation::INT16, Operation::INT32, Operation::INT64, 
                                            Operation::UINT8, Operation::UINT16, Operation::UINT32, Operation::UINT64,
                                            Operation::FLOAT, Operation::DOUBLE, Operation::CHAR, Operation::BOOLEAN};
    auto outputBufferPointer = outputBuffer.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}

TEST(MLIRNESIRTEST_TYPES, DISABLED_loadAndStoreAllBasicValuesInLoop) {
    const uint64_t numTuples = 2;
    auto buffMgr = std::make_shared<NES::Runtime::BufferManager>();
    // Create InputBuffer and fill it with data.
    auto inputBuffer = buffMgr->getBufferBlocking();
    struct __attribute__((packed)) testTupleStruct {
        int8_t a;
        int16_t b;
        int32_t c;
        int64_t d;
        uint8_t e;
        uint16_t f;
        uint32_t g;
        uint64_t h;
        float i;
        double j;
        char k;
        bool l;
    };
    testTupleStruct testTuplesArray[numTuples] = {
        testTupleStruct{8, 16, 32, 64, UINT8_MAX, UINT16_MAX, UINT32_MAX, UINT64_MAX, 4.2, -4.2, 'a', false},
        testTupleStruct{-8, -16, -32, -64, UINT8_MAX, UINT16_MAX, UINT32_MAX, UINT64_MAX, 8.4, -8.4, 'b', true}
    };
    auto inputBufferPointer = inputBuffer.getBuffer<testTupleStruct>();
    memcpy(inputBufferPointer, &testTuplesArray, sizeof(testTupleStruct) * numTuples);
    inputBuffer.setNumberOfTuples(numTuples);
    auto outputBuffer = buffMgr->getBufferBlocking();

    //Debug Print
    auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other,
                                                        "printValueFromMLIR",
                                                        std::vector<string>{"iOp"},
                                                        std::vector<Operation::BasicType>{},
                                                        Operation::BasicType::VOID);

    //Setup: Execute function args - A map that allows to save and reuse defined BBs - NESIR module.
    std::vector<Operation::BasicType> executeArgTypes{Operation::INT8PTR, Operation::INT8PTR};
    std::vector<std::string> executeArgNames{"inputTupleBuffer", "outputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Operation::INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {})
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
                ->addOperation(getProxyCallOperation(Operation::GetNumTuples, false))
                ->addOperation(make_shared<LoopOperation>(LoopOperation::ForLoop, std::vector<string>{"iOp"}))
                ->addLoopHeadBlock(
                    saveBB(createBB("loopHeadBB", 1, {}, "iOp"), savedBBs, "loopHeadBB")
                    ->addOperation(std::make_shared<CompareOperation>("loopHeadCompareOp", "iOp", "getNumTuplesOp", CompareOperation::ISLT))
                    ->addOperation(make_shared<IfOperation>("loopHeadCompareOp", std::vector<std::string>{"iOp"}, std::vector<std::string>{"iOp"}))
                    ->addThenBlock(
                        createBB("loopBodyBB", 2, {}, "iOp")
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp1", Operation::INT8,  44, 0, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp2", Operation::INT16, 44, 1, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp3", Operation::INT32, 44, 3, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp4", Operation::INT64, 44, 7, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp5", Operation::UINT8, 44, 15, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp6", Operation::UINT16, 44, 16, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp7", Operation::UINT32, 44, 18, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp8", Operation::UINT64, 44, 22, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp9", Operation::FLOAT,  44, 30, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp10", Operation::DOUBLE, 44, 34, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp11", Operation::CHAR, 44, 42, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp12", Operation::BOOLEAN, 44, 43, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<LoadOperation>("int8", "inBufAddrOp1"))
                        ->addOperation(std::make_shared<LoadOperation>("int16", "inBufAddrOp2"))
                        ->addOperation(std::make_shared<LoadOperation>("int32", "inBufAddrOp3"))
                        ->addOperation(std::make_shared<LoadOperation>("int64", "inBufAddrOp4"))
                        ->addOperation(std::make_shared<LoadOperation>("uint8", "inBufAddrOp5"))
                        ->addOperation(std::make_shared<LoadOperation>("uint16", "inBufAddrOp6"))
                        ->addOperation(std::make_shared<LoadOperation>("uint32", "inBufAddrOp7"))
                        ->addOperation(std::make_shared<LoadOperation>("uint64", "inBufAddrOp8"))
                        ->addOperation(std::make_shared<LoadOperation>("float", "inBufAddrOp9"))
                        ->addOperation(std::make_shared<LoadOperation>("double", "inBufAddrOp10"))
                        ->addOperation(std::make_shared<LoadOperation>("char", "inBufAddrOp11"))
                        ->addOperation(std::make_shared<LoadOperation>("bool", "inBufAddrOp12"))

                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp1", Operation::INT8,  44, 0, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp2", Operation::INT16, 44, 1, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp3", Operation::INT32, 44, 3, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp4", Operation::INT64, 44, 7, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp5", Operation::UINT8, 44, 15, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp6", Operation::UINT16, 44, 16, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp7", Operation::UINT32, 44, 18, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp8", Operation::UINT64, 44, 22, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp9", Operation::FLOAT,  44, 30, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp10", Operation::DOUBLE, 44, 34, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp11", Operation::CHAR, 44, 42, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp12", Operation::BOOLEAN, 44, 43, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<StoreOperation>("int8", "outBufAddrOp1"))
                        ->addOperation(std::make_shared<StoreOperation>("int16", "outBufAddrOp2"))
                        ->addOperation(std::make_shared<StoreOperation>("int32", "outBufAddrOp3"))
                        ->addOperation(std::make_shared<StoreOperation>("int64", "outBufAddrOp4"))
                        ->addOperation(std::make_shared<StoreOperation>("uint8", "outBufAddrOp5"))
                        ->addOperation(std::make_shared<StoreOperation>("uint16", "outBufAddrOp6"))
                        ->addOperation(std::make_shared<StoreOperation>("uint32", "outBufAddrOp7"))
                        ->addOperation(std::make_shared<StoreOperation>("uint64", "outBufAddrOp8"))
                        ->addOperation(std::make_shared<StoreOperation>("float", "outBufAddrOp9"))
                        ->addOperation(std::make_shared<StoreOperation>("double", "outBufAddrOp10"))
                        ->addOperation(std::make_shared<StoreOperation>("char", "outBufAddrOp11"))
                        ->addOperation(std::make_shared<StoreOperation>("bool", "outBufAddrOp12"))
                        ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"loopEndIncIOp"}))
                        ->addNextBlock(savedBBs["loopHeadBB"])
                    )
                    ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(make_shared<ReturnOperation>(0)))
                )
            );
    // std::cout << nesIR->toString() << '\n';
    // NESIR to MLIR
    auto mlirUtility = new MLIRUtility("", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);

    mlirUtility->runJit(false, addressof(inputBuffer), addressof(outputBuffer));

    // Print OutputBuffer after execution.
    std::vector<Operation::BasicType> types{Operation::INT8, Operation::INT16, Operation::INT32, Operation::INT64, 
                                            Operation::UINT8, Operation::UINT16, Operation::UINT32, Operation::UINT64,
                                            Operation::FLOAT, Operation::DOUBLE, Operation::CHAR, Operation::BOOLEAN};
    auto outputBufferPointer = outputBuffer.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}

TEST(MLIRNESIRTEST_TYPES, DISABLED_basicValueArithmetics) {
    const uint64_t numTuples = 2;
    auto buffMgr = std::make_shared<NES::Runtime::BufferManager>();
    // Create InputBuffer and fill it with data.
    auto inputBuffer = buffMgr->getBufferBlocking();
    struct __attribute__((packed)) testTupleStruct {
        int8_t a;
        int16_t b;
        int32_t c;
        int64_t d;
        uint8_t e;
        uint16_t f;
        uint32_t g;
        uint64_t h;
        float i;
        double j;
    };
    testTupleStruct testTuplesArray[numTuples] = {
        testTupleStruct{8, 16, 32, 64, INT8_MAX, INT16_MAX, INT32_MAX, INT64_MAX, 0.0, FLT_MAX},
        testTupleStruct{-8, -16, -32, -64, 8, 16, 32, 64, 4.2, -4.2}
    };
    auto inputBufferPointer = inputBuffer.getBuffer<testTupleStruct>();
    memcpy(inputBufferPointer, &testTuplesArray, sizeof(testTupleStruct) * numTuples);
    inputBuffer.setNumberOfTuples(numTuples);
    auto outputBuffer = buffMgr->getBufferBlocking();

    //Debug Print
    auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other,
                                                        "printValueFromMLIR",
                                                        std::vector<string>{"iOp"},
                                                        std::vector<Operation::BasicType>{},
                                                        Operation::BasicType::VOID);

    //Setup: Execute function args - A map that allows to save and reuse defined BBs - NESIR module.
    std::vector<Operation::BasicType> executeArgTypes{Operation::INT8PTR, Operation::INT8PTR};
    std::vector<std::string> executeArgNames{"inputTupleBuffer", "outputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Operation::INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {})
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
                ->addOperation(getProxyCallOperation(Operation::GetNumTuples, false))
                ->addOperation(make_shared<LoopOperation>(LoopOperation::ForLoop, std::vector<string>{"iOp"}))
                ->addLoopHeadBlock(
                    saveBB(createBB("loopHeadBB", 1, {}, "iOp"), savedBBs, "loopHeadBB")
                    ->addOperation(std::make_shared<CompareOperation>("loopHeadCompareOp", "iOp", "getNumTuplesOp", CompareOperation::ISLT))
                    ->addOperation(make_shared<IfOperation>("loopHeadCompareOp", std::vector<std::string>{"iOp"}, std::vector<std::string>{"iOp"}))
                    ->addThenBlock(
                        createBB("loopBodyBB", 2, {}, "iOp")
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp1", Operation::INT8,  42, 0, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp2", Operation::INT16, 42, 1, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp3", Operation::INT32, 42, 3, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp4", Operation::INT64, 42, 7, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp5", Operation::UINT8, 42, 15, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp6", Operation::UINT16, 42, 16, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp7", Operation::UINT32, 42, 18, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp8", Operation::UINT64, 42, 22, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp9", Operation::FLOAT,  42, 30, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp10", Operation::DOUBLE, 42, 34, "iOp", "getInDataBufOp"))
                        ->addOperation(std::make_shared<LoadOperation>("int8", "inBufAddrOp1"))
                        ->addOperation(std::make_shared<LoadOperation>("int16", "inBufAddrOp2"))
                        ->addOperation(std::make_shared<LoadOperation>("int32", "inBufAddrOp3"))
                        ->addOperation(std::make_shared<LoadOperation>("int64", "inBufAddrOp4"))
                        ->addOperation(std::make_shared<LoadOperation>("uint8", "inBufAddrOp5"))
                        ->addOperation(std::make_shared<LoadOperation>("uint16", "inBufAddrOp6"))
                        ->addOperation(std::make_shared<LoadOperation>("uint32", "inBufAddrOp7"))
                        ->addOperation(std::make_shared<LoadOperation>("uint64", "inBufAddrOp8"))
                        ->addOperation(std::make_shared<LoadOperation>("float", "inBufAddrOp9"))
                        ->addOperation(std::make_shared<LoadOperation>("double", "inBufAddrOp10"))
                        ->addOperation(std::make_shared<AddIntOperation>("addInt8", "int8", "int8"))
                        ->addOperation(std::make_shared<AddIntOperation>("addInt16", "int16", "int16"))
                        ->addOperation(std::make_shared<AddIntOperation>("addInt32", "int32", "int32"))
                        ->addOperation(std::make_shared<AddIntOperation>("addInt64", "int64", "int64"))
                        ->addOperation(std::make_shared<AddIntOperation>("addUInt8", "uint8", "uint8"))
                        ->addOperation(std::make_shared<AddIntOperation>("addUInt16", "uint16", "uint16"))
                        ->addOperation(std::make_shared<AddIntOperation>("addUInt32", "uint32", "uint32"))
                        ->addOperation(std::make_shared<AddIntOperation>("addUInt64", "uint64", "uint64"))
                        ->addOperation(std::make_shared<AddFloatOperation>("addFloat", "float", "float"))
                        ->addOperation(std::make_shared<AddFloatOperation>("addDouble", "double", "double"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp1", Operation::INT8,  42, 0, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp2", Operation::INT16, 42, 1, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp3", Operation::INT32, 42, 3, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp4", Operation::INT64, 42, 7, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp5", Operation::UINT8, 42, 15, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp6", Operation::UINT16, 42, 16, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp7", Operation::UINT32, 42, 18, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp8", Operation::UINT64, 42, 22, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp9", Operation::FLOAT,  42, 30, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp10", Operation::DOUBLE, 42, 34, "iOp", "getOutDataBufOp"))
                        ->addOperation(std::make_shared<StoreOperation>("addInt8", "outBufAddrOp1"))
                        ->addOperation(std::make_shared<StoreOperation>("addInt16", "outBufAddrOp2"))
                        ->addOperation(std::make_shared<StoreOperation>("addInt32", "outBufAddrOp3"))
                        ->addOperation(std::make_shared<StoreOperation>("addInt64", "outBufAddrOp4"))
                        ->addOperation(std::make_shared<StoreOperation>("addUInt8", "outBufAddrOp5"))
                        ->addOperation(std::make_shared<StoreOperation>("addUInt16", "outBufAddrOp6"))
                        ->addOperation(std::make_shared<StoreOperation>("addUInt32", "outBufAddrOp7"))
                        ->addOperation(std::make_shared<StoreOperation>("addUInt64", "outBufAddrOp8"))
                        ->addOperation(std::make_shared<StoreOperation>("addFloat", "outBufAddrOp9"))
                        ->addOperation(std::make_shared<StoreOperation>("addDouble", "outBufAddrOp10"))

                        
                        ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"loopEndIncIOp"}))
                        ->addNextBlock(savedBBs["loopHeadBB"])
                    )
                    ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(make_shared<ReturnOperation>(0)))
                )
            );
    // NESIR to MLIR
    auto mlirUtility = new MLIRUtility("", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);

    mlirUtility->runJit(false, addressof(inputBuffer), addressof(outputBuffer));

    // Print OutputBuffer after execution.
    std::vector<Operation::BasicType> types{Operation::INT8, Operation::INT16, Operation::INT32, Operation::INT64, 
                                            Operation::UINT8, Operation::UINT16, Operation::UINT32, Operation::UINT64,
                                            Operation::FLOAT, Operation::DOUBLE};
    auto outputBufferPointer = outputBuffer.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}


TEST(MLIRNESIRTEST_TYPES, basicValueAllComparisons) {
    const uint64_t numTuples = 2;
    auto buffMgr = std::make_shared<NES::Runtime::BufferManager>();
    // Create InputBuffer and fill it with data.
    auto inputBuffer = buffMgr->getBufferBlocking();
    struct __attribute__((packed)) testTupleStruct {
        int8_t a;
        int16_t b;
        int32_t c;
        int64_t d;
        uint8_t e;
        uint16_t f;
        uint32_t g;
        uint64_t h;
        float i;
        double j;
        char k;
        bool l;
    };
    testTupleStruct testTuplesArray[numTuples] = {
        testTupleStruct{8, 16, 32, 64, UINT8_MAX, UINT16_MAX, 31, UINT64_MAX, 4.2, (double)(1L << FLT_MANT_DIG) + 1.0, 'a', false},
        testTupleStruct{-8, -16, -32, -64, 7, 15, UINT32_MAX, 64, 4.19, (double)(1L << FLT_MANT_DIG), 'b', true}
    };
    auto inputBufferPointer = inputBuffer.getBuffer<testTupleStruct>();
    memcpy(inputBufferPointer, &testTuplesArray, sizeof(testTupleStruct) * numTuples);
    inputBuffer.setNumberOfTuples(numTuples);
    auto outputBuffer = buffMgr->getBufferBlocking();

    //Debug Print
    auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other,
                                                        "printValueFromMLIR",
                                                        std::vector<string>{"iOp"},
                                                        std::vector<Operation::BasicType>{},
                                                        Operation::BasicType::VOID);

    //Setup: Execute function args - A map that allows to save and reuse defined BBs - NESIR module.
    std::vector<Operation::BasicType> executeArgTypes{Operation::INT8PTR, Operation::INT8PTR};
    std::vector<std::string> executeArgNames{"inputTupleBuffer", "outputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Operation::INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {})
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
                ->addOperation(getProxyCallOperation(Operation::GetNumTuples, false))
                ->addOperation(make_shared<LoopOperation>(LoopOperation::ForLoop, std::vector<string>{"iOp"}))
                ->addLoopHeadBlock(
                    saveBB(createBB("loopHeadBB", 1, {Operation::INT64}, "iOp"), savedBBs, "loopHeadBB")
                    ->addOperation(std::make_shared<CompareOperation>("loopHeadCompareOp", "iOp", "getNumTuplesOp", CompareOperation::ISLT))
                    ->addOperation(make_shared<IfOperation>("loopHeadCompareOp", std::vector<std::string>{"iOp"}, std::vector<std::string>{"iOp"}))
                    ->addThenBlock(
                        createBB("loopBodyBB", 2, {}, "iOp")
                            ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp1", Operation::INT8,  44, 0, "iOp", "getInDataBufOp"))
                            ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp2", Operation::INT16, 44, 1, "iOp", "getInDataBufOp"))
                            ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp3", Operation::INT32, 44, 3, "iOp", "getInDataBufOp"))
                            ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp4", Operation::INT64, 44, 7, "iOp", "getInDataBufOp"))
                            ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp5", Operation::UINT8, 44, 15, "iOp", "getInDataBufOp"))
                            ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp6", Operation::UINT16, 44, 16, "iOp", "getInDataBufOp"))
                            ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp7", Operation::UINT32, 44, 18, "iOp", "getInDataBufOp"))
                            ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp8", Operation::UINT64, 44, 22, "iOp", "getInDataBufOp"))
                            ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp9", Operation::FLOAT, 44, 30, "iOp", "getInDataBufOp"))
                            ->addOperation(std::make_shared<AddressOperation>("inBufAddrOp10", Operation::DOUBLE, 44, 34, "iOp", "getInDataBufOp"))
                            ->addOperation(std::make_shared<LoadOperation>("int8", "inBufAddrOp1"))
                            ->addOperation(std::make_shared<LoadOperation>("int16", "inBufAddrOp2"))
                            ->addOperation(std::make_shared<LoadOperation>("int32", "inBufAddrOp3"))
                            ->addOperation(std::make_shared<LoadOperation>("int64", "inBufAddrOp4"))
                            ->addOperation(std::make_shared<LoadOperation>("intU8", "inBufAddrOp5"))
                            ->addOperation(std::make_shared<LoadOperation>("intU16", "inBufAddrOp6"))
                            ->addOperation(std::make_shared<LoadOperation>("intU32", "inBufAddrOp7"))
                            ->addOperation(std::make_shared<LoadOperation>("intU64", "inBufAddrOp8"))
                            ->addOperation(std::make_shared<LoadOperation>("float", "inBufAddrOp9"))
                            ->addOperation(std::make_shared<LoadOperation>("double", "inBufAddrOp10"))
                            // end of address- & loadOps
                            ->addOperation(std::make_shared<ConstIntOperation>("constI8CompareOp", 64, 8))
                            ->addOperation(make_shared<CompareOperation>("loopBodyIfCompareOp", "int8", "constI8CompareOp", CompareOperation::ISLT))
                            ->addOperation(make_shared<IfOperation>("loopBodyIfCompareOp", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenI8BB", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp1", Operation::INT8, 74, 0, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("int8", "outBufAddrOp1"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("I16IfBlock", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstIntOperation>("constI16CompareOp", 16, 16))
                            ->addOperation(make_shared<CompareOperation>("int16Compare", "int16", "constI16CompareOp", CompareOperation::ISLE))
                            ->addOperation(make_shared<IfOperation>("int16Compare", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenI16BB", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp2", Operation::INT16, 74, 1, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("int16", "outBufAddrOp2"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("I32IfBlock", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstIntOperation>("constI32CompareOp", 32, 32))
                            ->addOperation(make_shared<CompareOperation>("int32Compare", "int32", "constI32CompareOp", CompareOperation::ISGT))
                            ->addOperation(make_shared<IfOperation>("int32Compare", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenI32BB", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp3", Operation::INT32, 74, 3, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("int32", "outBufAddrOp3"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("I64IfBlock", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstIntOperation>("constI64CompareOp", 64, 64))
                            ->addOperation(make_shared<CompareOperation>("int64Compare", "int64", "constI64CompareOp", CompareOperation::INE))
                            ->addOperation(make_shared<IfOperation>("int64Compare", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenI64BB", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp4", Operation::INT64, 74, 7, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("int64", "outBufAddrOp4"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("IU8IfBlock", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstIntOperation>("constIU8CompareOp", 8, 8))
                            ->addOperation(make_shared<CompareOperation>("intU8Compare", "intU8", "constIU8CompareOp", CompareOperation::IULT))
                            ->addOperation(make_shared<IfOperation>("intU8Compare", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenIU8BB", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp5", Operation::UINT8, 74, 15, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("intU8", "outBufAddrOp5"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("IU16IfBlock", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstIntOperation>("constIU16CompareOp", 15, 16))
                            ->addOperation(make_shared<CompareOperation>("intU16Compare", "intU16", "constIU16CompareOp", CompareOperation::IUGT))
                            ->addOperation(make_shared<IfOperation>("intU16Compare", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenIU16BB", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp6", Operation::UINT16, 74, 16, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("intU16", "outBufAddrOp6"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("IU32IfBlock", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstIntOperation>("constIU32CompareOp", 31, 32))
                            ->addOperation(make_shared<CompareOperation>("intU32Compare", "intU32", "constIU32CompareOp", CompareOperation::IUGE))
                            ->addOperation(make_shared<IfOperation>("intU32Compare", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenIU32BB", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp7", Operation::UINT32, 74, 18, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("intU32", "outBufAddrOp7"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("IU64IfBlock", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstIntOperation>("constIU64CompareOp", 64, 64))
                            ->addOperation(make_shared<CompareOperation>("intU64Compare", "intU64", "constIU64CompareOp", CompareOperation::IULE))
                            ->addOperation(make_shared<IfOperation>("intU64Compare", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenIU64BB", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp8", Operation::UINT64, 74, 22, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("intU64", "outBufAddrOp8"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("floatIfBlock", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstFloatOperation>("constFloatCompareOp", 4.2, 32))
                            ->addOperation(make_shared<CompareOperation>("floatCompare", "float", "constFloatCompareOp", CompareOperation::FOEQ))
                            ->addOperation(make_shared<IfOperation>("floatCompare", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenFloatBB", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp9", Operation::FLOAT, 74, 30, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("float", "outBufAddrOp9"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("floatIfBlock2", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstFloatOperation>("constFloatCompareOp2", 4.2, 32))
                            ->addOperation(make_shared<CompareOperation>("floatCompare2", "float", "constFloatCompareOp2", CompareOperation::FONE))
                            ->addOperation(make_shared<IfOperation>("floatCompare2", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenFloatBB2", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp10", Operation::FLOAT, 74, 34, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("float", "outBufAddrOp10"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("floatIfBlock3", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstFloatOperation>("constFloatCompareOp3", 4.2, 32))
                            ->addOperation(make_shared<CompareOperation>("floatCompare3", "float", "constFloatCompareOp3", CompareOperation::FOLE))
                            ->addOperation(make_shared<IfOperation>("floatCompare3", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenFloatBB3", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp11", Operation::FLOAT, 74, 38, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("float", "outBufAddrOp11"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("doubleIfBlock", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstFloatOperation>("constDoubleCompareOp", (double)(1L << FLT_MANT_DIG), 64))
                            ->addOperation(make_shared<CompareOperation>("doubleCompare", "double", "constDoubleCompareOp", CompareOperation::FOLT))
                            ->addOperation(make_shared<IfOperation>("doubleCompare", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenDoubleBB", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp12", Operation::DOUBLE, 74, 42, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("double", "outBufAddrOp12"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("doubleIfBlock2", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstFloatOperation>("constDoubleCompareOp2", (double)(1L << FLT_MANT_DIG), 64))
                            ->addOperation(make_shared<CompareOperation>("doubleCompare2", "double", "constDoubleCompareOp2", CompareOperation::FOGE))
                            ->addOperation(make_shared<IfOperation>("doubleCompare2", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenDoubleBB2", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp13", Operation::DOUBLE, 74, 50, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("double", "outBufAddrOp13"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("doubleIfBlock3", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstFloatOperation>("constDoubleCompareOp3", (double)(1L << FLT_MANT_DIG), 64))
                            ->addOperation(make_shared<CompareOperation>("doubleCompare3", "double", "constDoubleCompareOp3", CompareOperation::FOGT))
                            ->addOperation(make_shared<IfOperation>("doubleCompare3", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenDoubleBB3", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp14", Operation::DOUBLE, 74, 58, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("double", "outBufAddrOp14"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("I32IfBlock2", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstIntOperation>("constI32CompareOp2", 32, 32))
                            ->addOperation(make_shared<CompareOperation>("int32Compare2", "int32", "constI32CompareOp2", CompareOperation::ISGE))
                            ->addOperation(make_shared<IfOperation>("int32Compare2", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenI32BB2", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp15", Operation::INT32, 74, 66, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("int32", "outBufAddrOp15"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                        ->addNextBlock(createBB("I32IfBlock3", 2, {}, "iOp")
                            ->addOperation(std::make_shared<ConstIntOperation>("constI32CompareOp3", 32, 32))
                            ->addOperation(make_shared<CompareOperation>("int32Compare3", "int32", "constI32CompareOp3", CompareOperation::IEQ))
                            ->addOperation(make_shared<IfOperation>("int32Compare3", std::vector<std::string>{"iOp"}, std::vector<std::string>{}))
                            ->addThenBlock(createBB("thenI32BB3", 3, {}, "iOp")
                                ->addOperation(std::make_shared<AddressOperation>("outBufAddrOp16", Operation::INT32, 74, 70, "iOp", "getOutDataBufOp"))
                                ->addOperation(std::make_shared<StoreOperation>("int32", "outBufAddrOp16"))
                                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                                ->addNextBlock(savedBBs["loopHeadBB"])
                            ))))))))))))))))
                        )))))))))))))))
                    )
                    ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(make_shared<ReturnOperation>(0)))
                )
            );
    std::cout << nesIR->toString() << '\n';
    // NESIR to MLIR
    auto mlirUtility = new MLIRUtility("", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);

    mlirUtility->runJit(false, addressof(inputBuffer), addressof(outputBuffer));

    // Print OutputBuffer after execution.
    std::vector<Operation::BasicType> types{Operation::INT8, Operation::INT16, Operation::INT32, Operation::INT64, 
                                            Operation::UINT8, Operation::UINT16, Operation::UINT32, Operation::UINT64,
                                            Operation::FLOAT, Operation::FLOAT, Operation::FLOAT, Operation::DOUBLE, 
                                            Operation::DOUBLE, Operation::DOUBLE, Operation::INT32, Operation::INT32};
    auto outputBufferPointer = outputBuffer.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}


}// namespace NES