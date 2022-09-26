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
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/AddressOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LoadOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/ProxyCallOperation.hpp>
#include <Nautilus/IR/Operations/ReturnOperation.hpp>
#include <Nautilus/IR/Operations/StoreOperation.hpp>
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
using namespace NES::Nautilus::IR;
using namespace NES::Nautilus::IR::Operations;

namespace NES {
class MLIRNESIRTest_ArithmeticOps : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MLIRNESIRTest.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("MLIRNESIRTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() { NES_INFO("MLIRNESIRTest test class TearDownTestCase."); }
};

void printBuffer(std::vector<Operations::PrimitiveStamp> types, uint64_t numTuples, int8_t* bufferPointer) {
    for (uint64_t i = 0; i < numTuples; ++i) {
        printf("------------\nTuple Nr. %lu\n------------\n", i + 1);
        for (auto type : types) {
            switch (type) {
                case PrimitiveStamp::INT1: {
                    printf("Value(INT1): %d \n", *bufferPointer);
                    bufferPointer += 1;
                    break;
                }
                case PrimitiveStamp::INT8: {
                    printf("Value(INT8): %d \n", *bufferPointer);
                    bufferPointer += 1;
                    break;
                }
                case PrimitiveStamp::INT16: {
                    int16_t* value = (int16_t*) bufferPointer;
                    printf("Value(INT16): %d \n", *value);
                    bufferPointer += 2;
                    break;
                }
                case PrimitiveStamp::INT32: {
                    int32_t* value = (int32_t*) bufferPointer;
                    printf("Value(INT32): %d \n", *value);
                    bufferPointer += 4;
                    break;
                }
                case PrimitiveStamp::INT64: {
                    int64_t* value = (int64_t*) bufferPointer;
                    printf("Value(INT64): %ld \n", *value);
                    bufferPointer += 8;
                    break;
                }
                case PrimitiveStamp::BOOLEAN: {
                    bool* value = (bool*) bufferPointer;
                    printf("Value(BOOL): %s \n", (*value) ? "true" : "false");
                    bufferPointer += 1;
                    break;
                }
                case PrimitiveStamp::CHAR: {
                    char* value = (char*) bufferPointer;
                    printf("Value(CHAR): %c \n", *value);
                    bufferPointer += 1;
                    break;
                }
                case PrimitiveStamp::FLOAT: {
                    float* value = (float*) bufferPointer;
                    printf("Value(FLOAT): %f \n", *value);
                    bufferPointer += 4;
                    break;
                }
                case PrimitiveStamp::DOUBLE: {
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
    std::vector<PrimitiveStamp> proxyDataBufferReturnArgs{PrimitiveStamp::INT8PTR};
    if (proxyCallType == ProxyCallOperation::GetDataBuffer) {
        if (getInputTB) {
            return make_shared<ProxyCallOperation>(Operation::GetDataBuffer,
                                                   "getInDataBufOp",
                                                   getInputDataBufArgs,
                                                   proxyDataBufferReturnArgs,
                                                   PrimitiveStamp::INT8PTR);
        } else {
            return make_shared<ProxyCallOperation>(Operation::GetDataBuffer,
                                                   "getOutDataBufOp",
                                                   getOutputDataBufArgs,
                                                   proxyDataBufferReturnArgs,
                                                   PrimitiveStamp::INT8PTR);
        }
    } else {
        return make_shared<ProxyCallOperation>(Operation::GetNumTuples,
                                               "getNumTuplesOp",
                                               getInputDataBufArgs,
                                               proxyDataBufferReturnArgs,
                                               PrimitiveStamp::INT64);
    }
}

pair<NES::Runtime::TupleBuffer, NES::Runtime::TupleBuffer> createInAndOutputBuffers() {
    const uint64_t numTuples = 2;
    auto buffMgr = new NES::Runtime::BufferManager();//shared pointer causes crash (destructor problem)

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
BasicBlockPtr createBB(std::string identifier, int level, std::vector<PrimitiveStamp> argTypes, Args... args) {
    std::vector<std::string> argList({args...});
    return make_shared<BasicBlock>(identifier, level, std::vector<OperationPtr>{}, argList, argTypes);
}

BasicBlockPtr
saveBB(BasicBlockPtr basicBlock, std::unordered_map<std::string, BasicBlockPtr>& savedBBs, const std::string& basicBlockName) {
    savedBBs.emplace(std::pair{basicBlockName, basicBlock});
    return savedBBs[basicBlockName];
}

/*
execute() {
    return 5 + 7
*/
TEST(MLIRNESIRTEST_IF, simpleNESIRIFAdd) {
    const uint64_t numTuples = 2;
    auto inAndOutputBuffer = createInAndOutputBuffers();

    //Debug Print
    auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other,
                                                        "printValueFromMLIR",
                                                        std::vector<string>{"iOp"},
                                                        std::vector<PrimitiveStamp>{},
                                                        PrimitiveStamp::VOID);

    //Setup: Execute function args - A map that allows to save and reuse defined BBs - NESIR module.
    std::vector<PrimitiveStamp> executeArgTypes{};
    std::vector<std::string> executeArgNames{};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Operations::INT64))
        ->addFunctionBasicBlock(createBB("executeBodyBB", 0, {})
                                    ->addOperation(std::make_shared<ConstIntOperation>("int1", 5, 64))
                                    ->addOperation(std::make_shared<ConstIntOperation>("int2", 4, 64))
                                    ->addOperation(std::make_shared<AddOperation>("add", "int1", "int2", Operations::INT64))
                                    ->addOperation(make_shared<ReturnOperation>(0)));

    // NESIR to MLIR
    auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);

    mlirUtility->runJit(false);

    // Print OutputBuffer after execution.
    std::vector<PrimitiveStamp> types{Operations::INT64};
    auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}

/*
execute(inTB, outTB) {
    inDatBuf = getDataBuffer(inTB)
    outDatBuf = getDataBuffer(outTB)
    numTuples = getNumTuples(inTB)
    i = constOp(0)
    const1 = constOp(1)

    while(i < numTuples)
        inAddr = addressOp(inDatBuf, i)
        loadedVal = loadValue(inAddr)
        const42 = constOp(42)
        addRes = addOp(const42, loadedVal)
        const46 = constOp(46)
        if(addRes < const46) {
            outAddr = addressOp(outDatBuf, i)
            storeOp(outAddr, addRes)
        }
        i = addOp(i, const1)
} (44, 0)
*/
TEST(MLIRNESIRTEST_IF, simpleNESIRIFtest) {
    const uint64_t numTuples = 2;
    auto inAndOutputBuffer = createInAndOutputBuffers();

    //Debug Print
    auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other,
                                                        "printValueFromMLIR",
                                                        std::vector<string>{"iOp"},
                                                        std::vector<PrimitiveStamp>{},
                                                        PrimitiveStamp::VOID);

    //Setup: Execute function args - A map that allows to save and reuse defined BBs - NESIR module.
    std::vector<PrimitiveStamp> executeArgTypes{Operations::INT8PTR, Operations::INT8PTR};
    std::vector<std::string> executeArgNames{"inputTupleBuffer", "outputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Operations::INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {}, "inputTupleBuffer", "outputTupleBuffer")
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
                ->addOperation(getProxyCallOperation(Operation::GetNumTuples, false))
                ->addOperation(make_shared<LoopOperation>(LoopOperation::ForLoop))
                ->addLoopHeadBlock(
                    saveBB(createBB("loopHeadBB", 1, {}, "iOp"), savedBBs, "loopHeadBB")
                        ->addOperation(std::make_shared<CompareOperation>("loopHeadCompareOp",
                                                                          "iOp",
                                                                          "getNumTuplesOp",
                                                                          CompareOperation::ISLT))
                        ->addOperation(make_shared<IfOperation>("loopHeadCompareOp",
                                                                std::vector<std::string>{"iOp"},
                                                                std::vector<std::string>{"iOp"}))
                        ->addThenBlock(
                            createBB("loopBodyBB", 2, {}, "iOp")
                                ->addOperation(
                                    make_shared<
                                        AddressOperation>("inputAddressOp", PrimitiveStamp::INT64, 9, 1, "iOp", "getInDataBufOp"))
                                ->addOperation(make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                                ->addOperation(std::make_shared<ConstIntOperation>("const42Op", 42, 64))
                                ->addOperation(
                                    std::make_shared<AddOperation>("loopBodyAddOp", "loadValOp", "const42Op", Operations::INT64))
                                ->addOperation(std::make_shared<ConstIntOperation>("const46Op", 46, 64))
                                ->addOperation(make_shared<CompareOperation>("loopBodyIfCompareOp",
                                                                             "loopBodyAddOp",
                                                                             "const46Op",
                                                                             CompareOperation::ISLT))
                                ->addOperation(make_shared<IfOperation>("loopBodyIfCompareOp",
                                                                        std::vector<std::string>{"iOp", "loopBodyAddOp"},
                                                                        std::vector<std::string>{}))
                                ->addThenBlock(
                                    createBB("thenBB", 3, {}, "iOp", "loopBodyAddOp")
                                        ->addOperation(make_shared<AddressOperation>("outputAddressOp",
                                                                                     PrimitiveStamp::INT64,
                                                                                     8,
                                                                                     0,
                                                                                     "iOp",
                                                                                     "getOutDataBufOp"))
                                        ->addOperation(make_shared<StoreOperation>("loopBodyAddOp", "outputAddressOp"))
                                        ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                                        ->addNextBlock(
                                            saveBB(createBB("loopEndBB", 2, {Operations::INT64}, "iOp"), savedBBs, "loopEndBB")
                                                ->addOperation(make_shared<AddOperation>(
                                                    "loopEndIncIOp",
                                                    "iOp",
                                                    "const1Op",
                                                    NES::Nautilus::IR::Operations::INT64))
                                                ->addOperation(
                                                    std::make_shared<BranchOperation>(std::vector<std::string>{"loopEndIncIOp"}))
                                                ->addNextBlock(savedBBs["loopHeadBB"]))))
                        ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(make_shared<ReturnOperation>(0)))));

    // NESIR to MLIR
    auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);

    mlirUtility->runJit(false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

    // Print OutputBuffer after execution.
    std::vector<PrimitiveStamp> types{Operations::INT64};
    auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}

/*
execute(inTB, outTB) {
    inDatBuf = getDataBuffer(inTB)
    outDatBuf = getDataBuffer(outTB)
    numTuples = getNumTuples(inTB)
    i = constOp(0)
    const1 = constOp(1)

    while(i < numTuples)
        inAddr = addressOp(inDatBuf, i)
        loadedVal = loadValue(inAddr)
        const42 = constOp(42)
        addRes = addOp(const42, loadedVal)
        const46 = constOp(46)
        outAddr = addressOp(outDatBuf, i)
        if(addRes < const46) {
            storeOp(outAddr, addRes)
        } else {
            const8 = constOp(8)
            elseAddResult = addOp(loadedVal, const8)
            storeOp(outAddr, addRes)
        }
        i = addOp(i, const1)
} (2, 4) -> (44, 54)
*/
TEST(MLIRNESIRTEST_IF, simpleNESIRIfElse) {
    const uint64_t numTuples = 2;
    auto inAndOutputBuffer = createInAndOutputBuffers();

    //Debug Print
    auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other,
                                                        "printValueFromMLIR",
                                                        std::vector<string>{"iOp"},
                                                        std::vector<PrimitiveStamp>{},
                                                        PrimitiveStamp::VOID);

    //Setup: Execute function args - A map that allows to save and reuse defined BBs - NESIR module.
    std::vector<PrimitiveStamp> executeArgTypes{Operations::INT8PTR, Operations::INT8PTR};
    std::vector<std::string> executeArgNames{"inputTupleBuffer", "outputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Operations::INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {}, "inputTupleBuffer", "outputTupleBuffer")
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
                ->addOperation(getProxyCallOperation(Operation::GetNumTuples, false))
                ->addOperation(make_shared<LoopOperation>(LoopOperation::ForLoop))
                ->addLoopHeadBlock(
                    saveBB(createBB("loopHeadBB", 1, {}, "iOp"), savedBBs, "loopHeadBB")
                        ->addOperation(std::make_shared<CompareOperation>("loopHeadCompareOp",
                                                                          "iOp",
                                                                          "getNumTuplesOp",
                                                                          CompareOperation::ISLT))
                        ->addOperation(make_shared<IfOperation>("loopHeadCompareOp",
                                                                std::vector<std::string>{"iOp"},
                                                                std::vector<std::string>{"iOp"}))
                        ->addThenBlock(
                            createBB("loopBodyBB", 2, {}, "iOp")
                                ->addOperation(
                                    make_shared<
                                        AddressOperation>("inputAddressOp", PrimitiveStamp::INT64, 9, 1, "iOp", "getInDataBufOp"))
                                ->addOperation(make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                                ->addOperation(std::make_shared<ConstIntOperation>("const42Op", 42, 64))
                                ->addOperation(
                                    std::make_shared<AddOperation>("loopBodyAddOp", "loadValOp", "const42Op", Operations::INT64))
                                ->addOperation(std::make_shared<ConstIntOperation>("const46Op", 46, 64))
                                ->addOperation(make_shared<AddressOperation>("outputAddressOp",
                                                                             PrimitiveStamp::INT64,
                                                                             8,
                                                                             0,
                                                                             "iOp",
                                                                             "getOutDataBufOp"))
                                ->addOperation(make_shared<CompareOperation>("loopBodyIfCompareOp",
                                                                             "loopBodyAddOp",
                                                                             "const46Op",
                                                                             CompareOperation::ISLT))
                                ->addOperation(make_shared<IfOperation>("loopBodyIfCompareOp",
                                                                        std::vector<std::string>{"iOp", "loopBodyAddOp"},
                                                                        std::vector<std::string>{}))
                                ->addThenBlock(
                                    createBB("thenBB", 3, {}, "iOp", "loopBodyAddOp", "outputAddressOp")
                                        ->addOperation(make_shared<StoreOperation>("loopBodyAddOp", "outputAddressOp"))
                                        ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                                        ->addNextBlock(
                                            saveBB(createBB("loopEndBB", 2, {Operations::INT64}, "iOp"), savedBBs, "loopEndBB")
                                                ->addOperation(make_shared<AddOperation>("loopEndIncIOp",
                                                                                         "iOp",
                                                                                         "const1Op",
                                                                                         Operations::INT64))
                                                ->addOperation(
                                                    std::make_shared<BranchOperation>(std::vector<std::string>{"loopEndIncIOp"}))
                                                ->addNextBlock(savedBBs["loopHeadBB"])))
                                ->addElseBlock(
                                    createBB("elseBB", 3, {}, "iOp", "loopBodyAddOp", "outputAddressOp")
                                        ->addOperation(std::make_shared<ConstIntOperation>("const8Op", 8, 64))
                                        ->addOperation(std::make_shared<AddOperation>("elseAddOp",
                                                                                      "loopBodyAddOp",
                                                                                      "const8Op",
                                                                                      Operations::INT64))
                                        ->addOperation(make_shared<StoreOperation>("elseAddOp", "outputAddressOp"))
                                        ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp"}))
                                        ->addNextBlock(savedBBs["loopEndBB"])))
                        ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(make_shared<ReturnOperation>(0)))));

    // NESIR to MLIR
    auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);

    mlirUtility->runJit(false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

    // Print OutputBuffer after execution.
    std::vector<PrimitiveStamp> types{Operations::INT64};
    auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}

/*
execute(inTB, outTB) {
    inDatBuf = getDataBuffer(inTB)
    outDatBuf = getDataBuffer(outTB)
    numTuples = getNumTuples(inTB)
    i = constOp(0)
    const1 = constOp(1)

    while(i < numTuples)
        inAddr = addressOp(inDatBuf, i)
        loadedVal = loadValue(inAddr)
        const42 = constOp(42)
        addRes = addOp(const42, loadedVal)
        const46 = constOp(46)
        if(addRes < const46) {
            ifAddResult = addOp(loadedVal, const46)
        } else {
            const8 = constOp(8)
            elseAddResult = addOp(loadedVal, const8)
        }
        outAddr = addressOp(outDatBuf, i)
        storeOp(outAddr, addRes)
        i = addOp(i, const1)
} (2,4) -> (90, 54)
*/
TEST(MLIRNESIRTEST_IF, simpleNESIRIfElseFollowUp) {
    const uint64_t numTuples = 2;
    auto inAndOutputBuffer = createInAndOutputBuffers();

    //Debug Print
    auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other,
                                                        "printValueFromMLIR",
                                                        std::vector<string>{"iOp"},
                                                        std::vector<PrimitiveStamp>{},
                                                        PrimitiveStamp::VOID);

    //Setup: Execute function args - A map that allows to save and reuse defined BBs - NESIR module.
    std::vector<PrimitiveStamp> executeArgTypes{Operations::INT8PTR, Operations::INT8PTR};
    std::vector<std::string> executeArgNames{"inputTupleBuffer", "outputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Operations::INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {}, "inputTupleBuffer", "outputTupleBuffer")
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
                ->addOperation(getProxyCallOperation(Operation::GetNumTuples, false))
                ->addOperation(make_shared<LoopOperation>(LoopOperation::ForLoop))
                ->addLoopHeadBlock(
                    saveBB(createBB("loopHeadBB", 1, {}, "iOp"), savedBBs, "loopHeadBB")
                        ->addOperation(std::make_shared<CompareOperation>("loopHeadCompareOp",
                                                                          "iOp",
                                                                          "getNumTuplesOp",
                                                                          CompareOperation::ISLT))
                        ->addOperation(make_shared<IfOperation>("loopHeadCompareOp",
                                                                std::vector<std::string>{"iOp"},
                                                                std::vector<std::string>{"iOp"}))
                        ->addThenBlock(
                            createBB("loopBodyBB", 2, {}, "iOp")
                                ->addOperation(
                                    make_shared<
                                        AddressOperation>("inputAddressOp", PrimitiveStamp::INT64, 9, 1, "iOp", "getInDataBufOp"))
                                ->addOperation(make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                                ->addOperation(std::make_shared<ConstIntOperation>("const42Op", 42, 64))
                                ->addOperation(
                                    std::make_shared<AddOperation>("loopBodyAddOp", "loadValOp", "const42Op", Operations::INT64))
                                ->addOperation(std::make_shared<ConstIntOperation>("const46Op", 46, 64))
                                ->addOperation(make_shared<CompareOperation>("loopBodyIfCompareOp",
                                                                             "loopBodyAddOp",
                                                                             "const46Op",
                                                                             CompareOperation::ISLT))
                                ->addOperation(make_shared<IfOperation>("loopBodyIfCompareOp",
                                                                        std::vector<std::string>{"iOp", "loopBodyAddOp"},
                                                                        std::vector<std::string>{}))
                                ->addThenBlock(
                                    createBB("thenBB", 3, {}, "iOp", "loopBodyAddOp")
                                        ->addOperation(std::make_shared<AddOperation>("thenAddOp",
                                                                                      "loopBodyAddOp",
                                                                                      "const46Op",
                                                                                      Operations::INT64))
                                        ->addOperation(
                                            std::make_shared<BranchOperation>(std::vector<std::string>{"iOp", "thenAddOp"}))
                                        ->addNextBlock(
                                            saveBB(createBB("loopEndBB",
                                                            2,
                                                            {Operations::INT64, Operations::INT64},
                                                            "iOp",
                                                            "ifAddResult"),
                                                   savedBBs,
                                                   "loopEndBB")
                                                ->addOperation(make_shared<AddressOperation>("outputAddressOp",
                                                                                             PrimitiveStamp::INT64,
                                                                                             8,
                                                                                             0,
                                                                                             "iOp",
                                                                                             "getOutDataBufOp"))
                                                ->addOperation(make_shared<StoreOperation>("ifAddResult", "outputAddressOp"))
                                                ->addOperation(make_shared<AddOperation>("loopEndIncIOp",
                                                                                         "iOp",
                                                                                         "const1Op",
                                                                                         Operations::INT64))
                                                ->addOperation(
                                                    std::make_shared<BranchOperation>(std::vector<std::string>{"loopEndIncIOp"}))
                                                ->addNextBlock(savedBBs["loopHeadBB"])))
                                ->addElseBlock(createBB("elseBB", 3, {}, "iOp", "loopBodyAddOp")
                                                   ->addOperation(std::make_shared<ConstIntOperation>("const8Op", 8, 64))
                                                   ->addOperation(std::make_shared<AddOperation>("elseAddOp",
                                                                                                 "loopBodyAddOp",
                                                                                                 "const8Op",
                                                                                                 Operations::INT64))
                                                   ->addOperation(std::make_shared<BranchOperation>(
                                                       std::vector<std::string>{"iOp", "elseAddOp"}))
                                                   ->addNextBlock(savedBBs["loopEndBB"])))
                        ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(make_shared<ReturnOperation>(0)))));

    // NESIR to MLIR
    auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);

    mlirUtility->runJit(false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

    // Print OutputBuffer after execution.
    std::vector<PrimitiveStamp> types{Operations::INT64};
    auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}

/*
execute(inTB, outTB) {
    inDatBuf = getDataBuffer(inTB)
    outDatBuf = getDataBuffer(outTB)
    numTuples = getNumTuples(inTB)
    i = constOp(0)
    const1 = constOp(1)

    while(i < numTuples)
        inAddr = addressOp(inDatBuf, i)
        loadedVal = loadValue(inAddr)
        const42 = constIntOp(42)
        addResult = addOp(const42, loadedVal)
        branchVal = constIntOp(0)
        const46 = constIntOp(46)
        if(addResult < const46) {
            addResult = addOp(loadedVal, const46)
            const100 = constIntOp(100)
            if(ifAddResult < const100) {
                branchVal = const1
            } else {
                branchVal = const46
                addResult = addOp(ifAddResult, const46)
            }
        } else {
            const8 = constOp(8)
            addResult = addOp(loadedVal, const8)
            branchVal = const8
        }
        addResult = addResult + branchVal
        outAddr = addressOp(outDatBuf, i)
        storeOp(outAddr, addRes)
        i = addOp(i, const1)
} (2, 4) -> (91, 62)
*/
TEST(MLIRNESIRTEST_IF_NESTED, NESIRIfElseNestedMultipleFollowUps) {
    const uint64_t numTuples = 2;
    auto inAndOutputBuffer = createInAndOutputBuffers();

    //Debug Print
    auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other,
                                                        "printValueFromMLIR",
                                                        std::vector<string>{"iOp"},
                                                        std::vector<PrimitiveStamp>{},
                                                        PrimitiveStamp::VOID);

    //Setup: Execute function args - A map that allows to save and reuse defined BBs - NESIR module.
    std::vector<PrimitiveStamp> executeArgTypes{Operations::INT8PTR, Operations::INT8PTR};
    std::vector<std::string> executeArgNames{"inputTupleBuffer", "outputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Operations::INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {}, "inputTupleBuffer", "outputTupleBuffer")
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
                ->addOperation(getProxyCallOperation(Operation::GetNumTuples, false))
                ->addOperation(make_shared<LoopOperation>(LoopOperation::ForLoop))
                ->addLoopHeadBlock(
                    saveBB(createBB("loopHeadBB", 1, {}, "iOp"), savedBBs, "loopHeadBB")
                        ->addOperation(std::make_shared<CompareOperation>("loopHeadCompareOp",
                                                                          "iOp",
                                                                          "getNumTuplesOp",
                                                                          CompareOperation::ISLT))
                        ->addOperation(make_shared<IfOperation>("loopHeadCompareOp",
                                                                std::vector<std::string>{"iOp"},
                                                                std::vector<std::string>{"iOp"}))
                        ->addThenBlock(
                            createBB("loopBodyBB", 2, {}, "iOp")
                                ->addOperation(
                                    make_shared<
                                        AddressOperation>("inputAddressOp", PrimitiveStamp::INT64, 9, 1, "iOp", "getInDataBufOp"))
                                ->addOperation(make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                                ->addOperation(std::make_shared<ConstIntOperation>("const42Op", 42, 64))
                                ->addOperation(
                                    std::make_shared<AddOperation>("loopBodyAddOp", "loadValOp", "const42Op", Operations::INT64))
                                ->addOperation(std::make_shared<ConstIntOperation>("branchVal", 0, 64))
                                ->addOperation(std::make_shared<ConstIntOperation>("const46Op", 46, 64))
                                ->addOperation(make_shared<CompareOperation>("loopBodyIfCompareOp",
                                                                             "loopBodyAddOp",
                                                                             "const46Op",
                                                                             CompareOperation::ISLT))
                                ->addOperation(
                                    make_shared<IfOperation>("loopBodyIfCompareOp",
                                                             std::vector<std::string>{"iOp", "loopBodyAddOp", "branchVal"},
                                                             std::vector<std::string>{"iOp", "loopBodyAddOp", "branchVal"}))
                                ->addThenBlock(
                                    createBB("thenBB", 3, {}, "iOp", "loopBodyAddOp", "branchVal")
                                        ->addOperation(std::make_shared<AddOperation>("thenAddOp",
                                                                                      "loopBodyAddOp",
                                                                                      "const46Op",
                                                                                      Operations::INT64))
                                        ->addOperation(std::make_shared<ConstIntOperation>("const100Op", 100, 64))
                                        ->addOperation(make_shared<CompareOperation>("nestedIfCompare",
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
                                                                    {Operations::INT64, Operations::INT64, Operations::INT64},
                                                                    "iOp",
                                                                    "ifAddResult",
                                                                    "ifBranchVal"),
                                                           savedBBs,
                                                           "loopEndBB")
                                                        ->addOperation(make_shared<AddOperation>("loopEndAddResult",
                                                                                                 "ifAddResult",
                                                                                                 "ifBranchVal",
                                                                                                 Operations::INT64))
                                                        ->addOperation(make_shared<AddressOperation>("outputAddressOp",
                                                                                                     PrimitiveStamp::INT64,
                                                                                                     8,
                                                                                                     0,
                                                                                                     "iOp",
                                                                                                     "getOutDataBufOp"))
                                                        ->addOperation(
                                                            make_shared<StoreOperation>("loopEndAddResult", "outputAddressOp"))
                                                        ->addOperation(make_shared<AddOperation>("loopEndIncIOp",
                                                                                                 "iOp",
                                                                                                 "const1Op",
                                                                                                 Operations::INT64))
                                                        ->addOperation(std::make_shared<BranchOperation>(
                                                            std::vector<std::string>{"loopEndIncIOp"}))
                                                        ->addNextBlock(savedBBs["loopHeadBB"])))
                                        ->addElseBlock(
                                            createBB("thenElseBB", 4, {}, "iOp", "thenAddOp", "branchVal")
                                                ->addOperation(std::make_shared<ConstIntOperation>("thenElseBranchVal", 47, 64))
                                                ->addOperation(std::make_shared<AddOperation>("thenElseAddOp",
                                                                                              "thenAddOp",
                                                                                              "thenElseBranchVal",
                                                                                              Operations::INT64))
                                                ->addOperation(std::make_shared<BranchOperation>(
                                                    std::vector<std::string>{"iOp", "thenElseAddOp", "thenElseBranchVal"}))
                                                ->addNextBlock(savedBBs["loopEndBB"])))
                                ->addElseBlock(createBB("elseBB", 3, {}, "iOp", "loopBodyAddOp", "branchVal")
                                                   ->addOperation(std::make_shared<ConstIntOperation>("const8Op", 8, 64))
                                                   ->addOperation(std::make_shared<AddOperation>("elseAddOp",
                                                                                                 "loopBodyAddOp",
                                                                                                 "const8Op",
                                                                                                 Operations::INT64))
                                                   ->addOperation(std::make_shared<BranchOperation>(
                                                       std::vector<std::string>{"iOp", "elseAddOp", "const8Op"}))
                                                   ->addNextBlock(savedBBs["loopEndBB"])))
                        ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(make_shared<ReturnOperation>(0)))));

    // NESIR to MLIR
    auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);

    mlirUtility->runJit(false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

    // Print OutputBuffer after execution.
    std::vector<PrimitiveStamp> types{Operations::INT64};
    auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}
/*
execute(inTB, outTB) {
    inDatBuf = getDataBuffer(inTB)
    outDatBuf = getDataBuffer(outTB)
    numTuples = getNumTuples(inTB)
    i = constOp(0)
    const1 = constOp(1)

    while(i < numTuples)
        inAddr = addressOp(inDatBuf, i)
        loadedVal = loadValue(inAddr)
        const42 = constIntOp(42)
        addResult = addOp(const42, loadedVal)
        branchVal = constIntOp(0)
        const46 = constIntOp(46)
        if(addResult < const46) {
            addResult = addOp(loadedVal, const46)
            const100 = constIntOp(100)
            if(ifAddResult < const100) {
                branchVal = const1
            } else {
                branchVal = const46
                addResult = addOp(ifAddResult, const46)
            }
            addResult = addOp(addResult, branchVal)
        } else {
            const8 = constOp(8)
            addResult = addOp(loadedVal, const8)
            branchVal = const8
        }
        addResult = addResult + branchVal
        outAddr = addressOp(outDatBuf, i)
        storeOp(outAddr, addRes)
        i = addOp(i, const1)
} (2, 4) -> (92, 62)
*/
TEST(MLIRNESIRTEST_IF_NESTED_FOLLOWUP, NESIRIfElseNestedMultipleFollowUps) {
    const uint64_t numTuples = 2;
    auto inAndOutputBuffer = createInAndOutputBuffers();

    //Debug Print
    auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other,
                                                        "printValueFromMLIR",
                                                        std::vector<string>{"iOp"},
                                                        std::vector<PrimitiveStamp>{},
                                                        PrimitiveStamp::VOID);

    //Setup: Execute function args - A map that allows to save and reuse defined BBs - NESIR module.
    std::vector<PrimitiveStamp> executeArgTypes{Operations::INT8PTR, Operations::INT8PTR};
    std::vector<std::string> executeArgNames{"inputTupleBuffer", "outputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Operations::INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {}, "inputTupleBuffer", "outputTupleBuffer")
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
                ->addOperation(getProxyCallOperation(Operation::GetNumTuples, false))
                ->addOperation(make_shared<LoopOperation>(LoopOperation::ForLoop))
                ->addLoopHeadBlock(
                    saveBB(createBB("loopHeadBB", 1, {}, "iOp"), savedBBs, "loopHeadBB")
                        ->addOperation(std::make_shared<CompareOperation>("loopHeadCompareOp",
                                                                          "iOp",
                                                                          "getNumTuplesOp",
                                                                          CompareOperation::ISLT))
                        ->addOperation(make_shared<IfOperation>("loopHeadCompareOp",
                                                                std::vector<std::string>{"iOp"},
                                                                std::vector<std::string>{"iOp"}))
                        ->addThenBlock(
                            createBB("loopBodyBB", 2, {}, "iOp")
                                ->addOperation(
                                    make_shared<
                                        AddressOperation>("inputAddressOp", PrimitiveStamp::INT64, 9, 1, "iOp", "getInDataBufOp"))
                                ->addOperation(make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                                ->addOperation(std::make_shared<ConstIntOperation>("const42Op", 42, 64))
                                ->addOperation(
                                    std::make_shared<AddOperation>("loopBodyAddOp", "loadValOp", "const42Op", Operations::INT64))
                                ->addOperation(std::make_shared<ConstIntOperation>("branchVal", 0, 64))
                                ->addOperation(std::make_shared<ConstIntOperation>("const46Op", 46, 64))
                                ->addOperation(make_shared<CompareOperation>("loopBodyIfCompareOp",
                                                                             "loopBodyAddOp",
                                                                             "const46Op",
                                                                             CompareOperation::ISLT))
                                ->addOperation(
                                    make_shared<IfOperation>("loopBodyIfCompareOp",
                                                             std::vector<std::string>{"iOp", "loopBodyAddOp", "branchVal"},
                                                             std::vector<std::string>{"iOp", "loopBodyAddOp", "branchVal"}))
                                ->addThenBlock(
                                    createBB("thenBB", 3, {}, "iOp", "loopBodyAddOp", "branchVal")
                                        ->addOperation(std::make_shared<AddOperation>("thenAddOp",
                                                                                      "loopBodyAddOp",
                                                                                      "const46Op",
                                                                                      Operations::INT64))
                                        ->addOperation(std::make_shared<ConstIntOperation>("const100Op", 100, 64))
                                        ->addOperation(make_shared<CompareOperation>("nestedIfCompare",
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
                                                                    {Operations::INT64, Operations::INT64, Operations::INT64},
                                                                    "iOp",
                                                                    "nestedIfAddResult",
                                                                    "ifBranchVal"),
                                                           savedBBs,
                                                           "nestedIfFollowUpBB")
                                                        ->addOperation(make_shared<AddOperation>("afterIfBBAddOp",
                                                                                                 "nestedIfAddResult",
                                                                                                 "ifBranchVal",
                                                                                                 Operations::INT64))
                                                        ->addOperation(std::make_shared<BranchOperation>(
                                                            std::vector<std::string>{"iOp", "afterIfBBAddOp", "ifBranchVal"}))
                                                        ->addNextBlock(
                                                            saveBB(createBB(
                                                                       "loopEndBB",
                                                                       2,
                                                                       {Operations::INT64, Operations::INT64, Operations::INT64},
                                                                       "iOp",
                                                                       "ifAddResult",
                                                                       "ifBranchVal"),
                                                                   savedBBs,
                                                                   "loopEndBB")
                                                                ->addOperation(make_shared<AddOperation>("loopEndAddResult",
                                                                                                         "ifAddResult",
                                                                                                         "ifBranchVal",
                                                                                                         Operations::INT64))
                                                                ->addOperation(
                                                                    make_shared<AddressOperation>("outputAddressOp",
                                                                                                  PrimitiveStamp::INT64,
                                                                                                  8,
                                                                                                  0,
                                                                                                  "iOp",
                                                                                                  "getOutDataBufOp"))
                                                                ->addOperation(make_shared<StoreOperation>("loopEndAddResult",
                                                                                                           "outputAddressOp"))
                                                                ->addOperation(make_shared<AddOperation>("loopEndIncIOp",
                                                                                                         "iOp",
                                                                                                         "const1Op",
                                                                                                         Operations::INT64))
                                                                ->addOperation(std::make_shared<BranchOperation>(
                                                                    std::vector<std::string>{"loopEndIncIOp"}))
                                                                ->addNextBlock(savedBBs["loopHeadBB"]))))
                                        ->addElseBlock(
                                            createBB("thenElseBB", 4, {}, "iOp", "thenAddOp", "branchVal")
                                                ->addOperation(std::make_shared<ConstIntOperation>("thenElseBranchVal", 47, 64))
                                                ->addOperation(std::make_shared<AddOperation>("thenElseAddOp",
                                                                                              "thenAddOp",
                                                                                              "thenElseBranchVal",
                                                                                              Operations::INT64))
                                                ->addOperation(std::make_shared<BranchOperation>(
                                                    std::vector<std::string>{"iOp", "thenElseAddOp", "thenElseBranchVal"}))
                                                ->addNextBlock(savedBBs["nestedIfFollowUpBB"])))
                                ->addElseBlock(createBB("elseBB", 3, {}, "iOp", "loopBodyAddOp", "branchVal")
                                                   ->addOperation(std::make_shared<ConstIntOperation>("const8Op", 8, 64))
                                                   ->addOperation(std::make_shared<AddOperation>("elseAddOp",
                                                                                                 "loopBodyAddOp",
                                                                                                 "const8Op",
                                                                                                 Operations::INT64))
                                                   ->addOperation(std::make_shared<BranchOperation>(
                                                       std::vector<std::string>{"iOp", "elseAddOp", "const8Op"}))
                                                   ->addNextBlock(savedBBs["loopEndBB"])))
                        ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(make_shared<ReturnOperation>(0)))));

    // NESIR to MLIR
    auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);

    mlirUtility->runJit(false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

    // Print OutputBuffer after execution.
    std::vector<PrimitiveStamp> types{Operations::INT64};
    auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}

/*
execute(inTB, outTB) {
    inDatBuf = getDataBuffer(inTB)
    outDatBuf = getDataBuffer(outTB)
    numTuples = getNumTuples(inTB)
    i = constOp(0)
    const1 = constOp(1)

    while(i < numTuples)
        inAddr = addressOp(inDatBuf, i)
        loadedVal = loadValue(inAddr)
        const42 = constIntOp(42)
        addResult = addOp(const42, loadedVal)
        branchVal = constIntOp(0)
        const46 = constIntOp(46)
        if(addResult < const46) {
            addResult = addOp(loadedVal, const46)
            const100 = constIntOp(100)
            if(ifAddResult < const100) {
                branchVal = const1
            } else {
                branchVal = const46
                addResult = addOp(ifAddResult, const46)
            }
            addResult = addOp(addResult, branchVal)
            while(j < branchVal) {
                addRes = addRes + addRes
                proxyCall_Print(addRes)
                j = addOp(j, const1)
            }
        } else {
            const8 = constOp(8)
            addResult = addOp(loadedVal, const8)
            branchVal = const8
        }
        addResult = addResult + branchVal
        outAddr = addressOp(outDatBuf, i)
        storeOp(outAddr, addRes)
        i = addOp(i, const1)
} (2, 4) -> (92, 62)
*/
TEST(MLIRNESIRTEST_NESTED_WITH_LOOP, NESIRIfElseNestedLoop) {
    const uint64_t numTuples = 2;
    auto inAndOutputBuffer = createInAndOutputBuffers();

    //Setup: Execute function args - A map that allows to save and reuse defined BBs - NESIR module.
    std::vector<PrimitiveStamp> executeArgTypes{Operations::INT8PTR, Operations::INT8PTR};
    std::vector<std::string> executeArgNames{"inputTupleBuffer", "outputTupleBuffer"};
    std::unordered_map<std::string, BasicBlockPtr> savedBBs;
    auto nesIR = std::make_shared<NESIR>();

    nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, Operations::INT64))
        ->addFunctionBasicBlock(
            createBB("executeBodyBB", 0, {}, "inputTupleBuffer", "outputTupleBuffer")
                ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
                ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
                ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
                ->addOperation(getProxyCallOperation(Operation::GetNumTuples, false))
                ->addOperation(make_shared<LoopOperation>(LoopOperation::ForLoop))
                ->addLoopHeadBlock(
                    saveBB(createBB("loopHeadBB", 1, {}, "iOp"), savedBBs, "loopHeadBB")
                        ->addOperation(std::make_shared<CompareOperation>("loopHeadCompareOp",
                                                                          "iOp",
                                                                          "getNumTuplesOp",
                                                                          CompareOperation::ISLT))
                        ->addOperation(make_shared<IfOperation>("loopHeadCompareOp",
                                                                std::vector<std::string>{"iOp"},
                                                                std::vector<std::string>{"iOp"}))
                        ->addThenBlock(
                            createBB("loopBodyBB", 2, {}, "iOp")
                                ->addOperation(
                                    make_shared<
                                        AddressOperation>("inputAddressOp", PrimitiveStamp::INT64, 9, 1, "iOp", "getInDataBufOp"))
                                ->addOperation(make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                                ->addOperation(std::make_shared<ConstIntOperation>("const42Op", 42, 64))
                                ->addOperation(
                                    std::make_shared<AddOperation>("loopBodyAddOp", "loadValOp", "const42Op", Operations::INT64))
                                ->addOperation(std::make_shared<ConstIntOperation>("branchVal", 0, 64))
                                ->addOperation(std::make_shared<ConstIntOperation>("const46Op", 46, 64))
                                ->addOperation(make_shared<CompareOperation>("loopBodyIfCompareOp",
                                                                             "loopBodyAddOp",
                                                                             "const46Op",
                                                                             CompareOperation::ISLT))
                                ->addOperation(
                                    make_shared<IfOperation>("loopBodyIfCompareOp",
                                                             std::vector<std::string>{"iOp", "loopBodyAddOp", "branchVal"},
                                                             std::vector<std::string>{"iOp", "loopBodyAddOp", "branchVal"}))
                                ->addThenBlock(
                                    createBB("thenBB", 3, {}, "iOp", "loopBodyAddOp", "branchVal")
                                        ->addOperation(std::make_shared<AddOperation>("thenAddOp",
                                                                                      "loopBodyAddOp",
                                                                                      "const46Op",
                                                                                      Operations::INT64))
                                        ->addOperation(std::make_shared<ConstIntOperation>("const100Op", 100, 64))
                                        ->addOperation(std::make_shared<ConstIntOperation>("jOp", 0, 64))
                                        ->addOperation(make_shared<CompareOperation>("nestedIfCompare",
                                                                                     "thenAddOp",
                                                                                     "const100Op",
                                                                                     CompareOperation::ISLT))
                                        ->addOperation(make_shared<IfOperation>(
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
                                                                    {Operations::INT64,
                                                                     Operations::INT64,
                                                                     Operations::INT64,
                                                                     Operations::INT64},
                                                                    "iOp",
                                                                    "jOp",
                                                                    "nestedIfAddResult",
                                                                    "ifBranchVal"),
                                                           savedBBs,
                                                           "nestedIfFollowUpBB")
                                                        ->addOperation(make_shared<AddOperation>("afterIfBBAddOp",
                                                                                                 "nestedIfAddResult",
                                                                                                 "ifBranchVal",
                                                                                                 Operations::INT64))
                                                        ->addOperation(make_shared<LoopOperation>(
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
                                                                ->addOperation(make_shared<IfOperation>(
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
                                                                            make_shared<AddOperation>("nestedLoopAddOp",
                                                                                                      "loopAddInput",
                                                                                                      "loopAddInput",
                                                                                                      Operations::INT64))
                                                                        ->addOperation(make_shared<ProxyCallOperation>(
                                                                            Operation::ProxyCallType::Other,
                                                                            "printValueFromMLIR",
                                                                            std::vector<string>{"nestedLoopAddOp"},
                                                                            std::vector<PrimitiveStamp>{},
                                                                            PrimitiveStamp::VOID))
                                                                        ->addOperation(
                                                                            make_shared<AddOperation>("nestedLoopEndIncJOp",
                                                                                                      "jOp",
                                                                                                      "const1Op",
                                                                                                      Operations::INT64))
                                                                        ->addOperation(std::make_shared<BranchOperation>(
                                                                            std::vector<std::string>{"iOp",
                                                                                                     "nestedLoopEndIncJOp",
                                                                                                     "loopAddInput",
                                                                                                     "nestedLoopUpperLimit"}))
                                                                        ->addNextBlock(savedBBs["nestedLoopHeadBB"]))
                                                                ->addElseBlock(
                                                                    saveBB(createBB("loopEndBB",
                                                                                    2,
                                                                                    {Operations::INT64,
                                                                                     Operations::INT64,
                                                                                     Operations::INT64},
                                                                                    "iOp",
                                                                                    "ifAddResult",
                                                                                    "ifBranchVal"),
                                                                           savedBBs,
                                                                           "loopEndBB")
                                                                        ->addOperation(
                                                                            make_shared<AddOperation>("loopEndAddResult",
                                                                                                      "ifAddResult",
                                                                                                      "ifBranchVal",
                                                                                                      Operations::INT64))
                                                                        ->addOperation(
                                                                            make_shared<AddressOperation>("outputAddressOp",
                                                                                                          PrimitiveStamp::INT64,
                                                                                                          8,
                                                                                                          0,
                                                                                                          "iOp",
                                                                                                          "getOutDataBufOp"))
                                                                        ->addOperation(
                                                                            make_shared<StoreOperation>("loopEndAddResult",
                                                                                                        "outputAddressOp"))
                                                                        ->addOperation(
                                                                            make_shared<AddOperation>("loopEndIncIOp",
                                                                                                      "iOp",
                                                                                                      "const1Op",
                                                                                                      Operations::INT64))
                                                                        ->addOperation(std::make_shared<BranchOperation>(
                                                                            std::vector<std::string>{"loopEndIncIOp"}))
                                                                        ->addNextBlock(savedBBs["loopHeadBB"])))))
                                        ->addElseBlock(
                                            createBB("thenElseBB", 4, {}, "iOp", "jOp", "thenAddOp", "branchVal")
                                                ->addOperation(std::make_shared<ConstIntOperation>("thenElseBranchVal", 47, 64))
                                                ->addOperation(std::make_shared<AddOperation>("thenElseAddOp",
                                                                                              "thenAddOp",
                                                                                              "thenElseBranchVal",
                                                                                              Operations::INT64))
                                                ->addOperation(std::make_shared<BranchOperation>(
                                                    std::vector<std::string>{"iOp", "jOp", "thenElseAddOp", "thenElseBranchVal"}))
                                                ->addNextBlock(savedBBs["nestedIfFollowUpBB"])))
                                ->addElseBlock(createBB("elseBB", 3, {}, "iOp", "loopBodyAddOp", "branchVal")
                                                   ->addOperation(std::make_shared<ConstIntOperation>("const8Op", 8, 64))
                                                   ->addOperation(std::make_shared<AddOperation>("elseAddOp",
                                                                                                 "loopBodyAddOp",
                                                                                                 "const8Op",
                                                                                                 Operations::INT64))
                                                   ->addOperation(std::make_shared<BranchOperation>(
                                                       std::vector<std::string>{"iOp", "elseAddOp", "const8Op"}))
                                                   ->addNextBlock(savedBBs["loopEndBB"])))
                        ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(make_shared<ReturnOperation>(0)))));

    // NESIR to MLIR
    auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
    assert(loadedModuleSuccess == 0);

    mlirUtility->runJit(false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

    // Print OutputBuffer after execution.
    std::vector<PrimitiveStamp> types{Operations::INT64};
    auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}

}// namespace NES