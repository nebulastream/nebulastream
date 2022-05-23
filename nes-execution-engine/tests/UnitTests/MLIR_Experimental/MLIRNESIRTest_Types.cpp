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

void* getInputBuffer(NES::Runtime::BufferManager* buffMgr) {
    const uint64_t numTuples = 2;
    auto inputBuffer = buffMgr->getBufferBlocking();

    // Create testTupleStruct. Load inputBuffer with testTupleStruct. Write sample array with testTupleStruct to inputBuffer.
    struct __attribute__((packed)) testTupleStruct{
        int64_t a;
    };
    testTupleStruct testTuplesArray[numTuples] = { testTupleStruct{1}, testTupleStruct{4} };
    auto inputBufferPointer = inputBuffer.getBuffer<testTupleStruct>();
    memcpy(inputBufferPointer, &testTuplesArray, sizeof(testTupleStruct) * numTuples);
    inputBuffer.setNumberOfTuples(numTuples);

    // Return inputBuffer pointer as void*.
    return addressof(inputBuffer);
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

//==----------------------------------------------------------------------==//
//==-------------------------- IF ELSE TESTS -----------------------------==//
//==----------------------------------------------------------------------==//

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
    printf("Starting Test NESIRSimpleTypeTest");
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
        ->addLoopTerminatorOpHeadBlock(
            saveBB(createBB("loopHeadBB", 1, "iOp"), savedBBs, "loopHeadBB")
            ->addOperation(std::make_shared<CompareOperation>("loopHeadCompareOp", "iOp", "getNumTuplesOp", CompareOperation::ISLT))
            ->addOperation(make_shared<IfOperation>("loopHeadCompareOp", std::vector<std::string>{"iOp"}, std::vector<std::string>{"iOp"}))
            ->addIfTerminatorOpThenBlock(
                createBB("loopBodyBB", 2, "iOp")
                ->addOperation(make_shared<AddressOperation>("inputAddressOp", NES::Operation::BasicType::INT64, 9, 1, "iOp", "getInDataBufOp"))
                ->addOperation(make_shared<LoadOperation>("loadValOp", "inputAddressOp"))
                ->addOperation(std::make_shared<ConstantIntOperation>("ifCompareConstOp", 20, 64))
                ->addOperation(std::make_shared<CompareOperation>("loopBodyIfCompareOp", "loadValOp", "ifCompareConstOp", CompareOperation::ISLT))
                ->addOperation(make_shared<IfOperation>("loopBodyIfCompareOp", std::vector<std::string>{"iOp", "loadValOp"}, std::vector<std::string>{"iOp", "loadValOp"}))
                ->addIfTerminatorOpThenBlock(
                    createBB("thenBB", 3, "iOp", "loadValOp")
                    ->addOperation(std::make_shared<ConstantIntOperation>("thenAddConstOp", 10, 64))
                    ->addOperation(std::make_shared<AddIntOperation>("thenAddOp", "loadValOp", "thenAddConstOp"))
                    ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp", "thenAddOp"}))
                    ->addBranchTerminatorOpNextBlock(
                saveBB(createBB("loopEndBB", 2, "iOp", "loadValOp"), savedBBs, "loopEndBB")
                ->addOperation(make_shared<AddressOperation>("outputAddressOp", NES::Operation::BasicType::INT64, 8, 0, "iOp", "getOutDataBufOp"))
                ->addOperation(make_shared<StoreOperation>("loadValOp", "outputAddressOp"))
                ->addOperation(make_shared<AddIntOperation>("loopEndIncIOp", "iOp", "const1Op"))
                ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"loopEndIncIOp"}))
                ->addBranchTerminatorOpNextBlock(savedBBs["loopHeadBB"])
                    )
                )
                ->addIfTerminatorOpElseBlock(
                    createBB("elseBB", 3, "iOp", "loadValOp")
                    ->addOperation(std::make_shared<ConstantIntOperation>("elseAddConstOp", 30, 64))
                    ->addOperation(make_shared<AddIntOperation>("elseAddOp", "loadValOp", "elseAddConstOp"))
                    ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"iOp", "elseAddOp"}))
                    ->addBranchTerminatorOpNextBlock(savedBBs["loopEndBB"])
                )
            )
        ->addIfTerminatorOpElseBlock(
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

// TEST(DISABLED_MLIRNESIRTEST_TYPES, NESIRIfElseNestedMultipleFollowUps) {
//     const uint64_t numTuples = 2;
//     auto inAndOutputBuffer = createInAndOutputBuffers();

//     //Debug Print
//     std::vector<string> proxyArgNames{"iOp"};
//     std::vector<Operation::BasicType> proxyArgTypes{};
//     auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other, "printValueFromMLIR", proxyArgNames,
//                                                         proxyArgTypes, Operation::BasicType::VOID);

//     // 1. Return Block
//     BasicBlockPtr returnBlock =  createBB("executeReturnBB", 1);
//     returnBlock->addOperation(make_shared<ReturnOperation>(0));

//         // 2.1 LoopEndBB
//         BasicBlockPtr loopEndBB = createBB("loopEndBB", 2, "iOp", "loadValOp");
//         loopEndBB->addOperation(make_shared<AddressOperation>("outputAddressOp", NES::Operation::BasicType::INT64, 8, 0, "iOp", "getOutDataBufOp"))
//                  ->addOperation(make_shared<StoreOperation>("loadValOp", "outputAddressOp"))
//                  ->addOperation(make_shared<AddIntOperation>("loopEndIncIOp", "iOp", "const1Op"));

//             // 3. ThenBlock
//             // BasicBlockPtr thenBB = make_shared<BasicBlock>("thenBB", std::vector<OperationPtr>{}, std::vector<std::string>{"iOp", "loadValOp"}, 3);
//             BasicBlockPtr thenBB = createBB("thenBB", 3, "iOp", "loadValOp");
//             thenBB->addOperation(std::make_shared<ConstantIntOperation>("thenAddConstOp", 10, 64))
//                   ->addOperation(std::make_shared<AddIntOperation>("thenAddOp", "loadValOp", "thenAddConstOp"))
//                   ->addOperation(std::make_shared<BranchOperation>(loopEndBB, std::vector<std::string>{"iOp", "thenAddOp"}));

//             // 4. ElseBlock
//             BasicBlockPtr elseBB = createBB("elseBB", 3, "iOp", "loadValOp");
//             elseBB->addOperation(std::make_shared<ConstantIntOperation>("elseAddConstOp", 30, 64))
//                   ->addOperation(make_shared<AddIntOperation>("elseAddOp", "loadValOp", "elseAddConstOp"))
//                   ->addOperation(make_shared<BranchOperation>(loopEndBB, std::vector<std::string>{"i", "elseAddOp"}));

//         // 5. LoopBodyBB & LoopBody Ops
//         BasicBlockPtr loopBodyBB = createBB("loopBodyBB", 2, "iOp");
//         loopBodyBB->addOperation(make_shared<AddressOperation>("inputAddressOp", NES::Operation::BasicType::INT64, 9, 1, "iOp", "getInDataBufOp"))
//                   ->addOperation(make_shared<LoadOperation>("loadValOp", loopBodyBB->getOperations().back(), "inputAddressOp"))
//                   ->addOperation(std::make_shared<ConstantIntOperation>("ifCompareConstOp", 20, 64))
//                   ->addOperation(std::make_shared<CompareOperation>("loopBodyIfCompareOp", "loadValOp", "ifCompareConstOp", CompareOperation::ISLT))
//                   ->addOperation(make_shared<IfOperation>("loopBodyIfCompareOp", thenBB, elseBB, std::vector<std::string>{"iOp", "loadValOp"}, std::vector<std::string>{"iOp", "loadValOp"}));

//     // 6. LoopHeadBB Ops
//     BasicBlockPtr loopHeadBB = createBB("loopHeadBB", 1, "iOp");
//     loopHeadBB->addOperation(std::make_shared<CompareOperation>("loopHeadCompareOp", "iOp", "getNumTuplesOp", CompareOperation::ISLT))
//               ->addOperation(make_shared<IfOperation>("loopHeadCompareOp", loopBodyBB, returnBlock, std::vector<std::string>{"iOp"}, std::vector<std::string>{"iOp"}));

//     // 7. ExecuteBB Ops
//     BasicBlockPtr executeBodyBB = createBB("executeBodyBB", 0, "inputTupleBuffer", "outputTupleBuffer");
//     executeBodyBB->addOperation(std::make_shared<ConstantIntOperation>("iOp", 0, 64))
//                  ->addOperation(std::make_shared<ConstantIntOperation>("const1Op", 1, 64))
//                  ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
//                  ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
//                  ->addOperation(getProxyCallOperation(Operation::GetNumTuples, false))
//                  ->addOperation(make_shared<LoopOperation>(LoopOperation::ForLoop, loopHeadBB));
//     std::shared_ptr<FunctionOperation> executeFuncOp = make_shared<FunctionOperation>(
//                     "execute", executeBodyBB, std::vector<Operation::BasicType>{Operation::INT8PTR, Operation::INT8PTR}, 
//                     std::vector<std::string>{"inputTupleBuffer", "outputTupleBuffer"}, Operation::INT64);

//     // 2.2 Close gap: Add latch back to LoopHead from LoopEndBB.
//     OperationPtr loopEndBBTerminatorOp = make_shared<BranchOperation>(loopHeadBB, std::vector<std::string>{"loopEndIncIOp"});
//     loopEndBB->addOperation(loopEndBBTerminatorOp);

// //-----------------------------------------------------------------//
// //-----------------------------------------------------------------//

//      // NESIR
//     auto nesIR = std::make_shared<NES::NESIR>(executeFuncOp);

//     // NESIR to MLIR
//     auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
//     int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
//     assert(loadedModuleSuccess == 0);

//     mlirUtility->runJit(false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

//     // Print OutputBuffer after execution.
//     std::vector<Operation::BasicType> types{Operation::INT64};
//     auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
//     printBuffer(types, numTuples, outputBufferPointer);
// }

// // If-Else test with loopOp after nested if-else, in Then block of parent if-else
// TEST(DISABLED_MLIRNESIRTEST_IF, NESIRIfElseNestedMultipleFollowUps) {
//     const uint64_t numTuples = 2;
//     auto inAndOutputBuffer = createInAndOutputBuffers();

//     //Debug Print
//     std::vector<string> proxyArgNames{"inputAddOp"};
//     std::vector<Operation::BasicType> proxyArgTypes{};
//     auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other, "printValueFromMLIR", proxyArgNames,
//                                                         proxyArgTypes, Operation::BasicType::VOID);

//     // Loop BodyBlock Operations
//     // Todo: AddressOp should access values at "i" to get "currentRecordIdx"
//     auto inputAddressOp = make_shared<AddressOperation>("inTBAddressOp", NES::Operation::BasicType::INT64, 9, 1, "i", "inputDataBuffer");
//     auto loadOp = make_shared<LoadOperation>("loadTBValOp", inputAddressOp, "inTBAddressOp");
//     auto addOp = make_shared<AddIntOperation>("inputAddOp", "loadTBValOp", "const47");
    

//     // Loop BodyBlock up to (not including) IF Operation
//     OperationPtr loopBodyCompOp = make_shared<CompareOperation>("loopCompare", "inputAddOp", "const50", CompareOperation::ISLT);
//     std::vector<OperationPtr> loopOps{inputAddressOp, loadOp, addOp, proxyPrintOp, loopBodyCompOp}; //IfOp added later
//     std::vector<string> loopArguments{"inputDataBuffer", "outputDataBuffer", "numTuples", "i", "const1Op", "const47", "const50", 
//                                  "const100", "const8", "nestedIfBranchConst"};
//     BasicBlockPtr loopBodyBlock = make_shared<BasicBlock>("loopBlock", loopOps, loopArguments, 2);

//     // Create Loop Operation
//     std::vector<string> loopHeaderArgs{"inputDataBuffer", "outputDataBuffer", "i", "const1Op", "const47", "const50", "const100", 
//                                   "const8", "numTuples", "nestedIfBranchConst"};
//     shared_ptr<LoopOperation> loopOperation = createTopLevelLoopOp(loopHeaderArgs, loopBodyBlock);

//     auto loopEndAdd = make_shared<AddIntOperation>("loopEndAdd", "inputAddOp", "nestedIfBranchConst");
//     auto outputAddressOp = make_shared<AddressOperation>("outTBAddressOp", NES::Operation::BasicType::INT64, 8, 0, "i", "outputDataBuffer");
//     auto storeOp = make_shared<StoreOperation>("loopEndAdd", "outTBAddressOp");
//     auto storeOp2 = make_shared<StoreOperation>("nestedIfBranchConst", "outTBAddressOp");
//     std::vector<OperationPtr> loopEndOps{loopEndAdd, outputAddressOp, storeOp, storeOp2};
//     std::vector<string> loopEndArgs{"inputAddOp", "nestedIfBranchConst", "outputDataBuffer"};
//     BasicBlockPtr loopEndBlock = createSpecialLoopEnd(loopOperation->getLoopHeaderBlock(), loopEndOps, loopEndArgs);

//     // Create If-only If operation
//     // Then Block Ops & Args:
//     //==--------- NESTED IF ------------==/
// //==--------- NESTED LOOP OPERATION!!!! ------------==/
//     auto nestedLoopAdd = make_shared<AddIntOperation>("nestedLoopAdd", "inputAddOp", "nestedIfBranchConst");
//     auto nestedLoopIncAdd = make_shared<AddIntOperation>("nestedLoopIncAdd", "j", "const1Op");

//     std::vector<OperationPtr> nestedLoopOps{nestedLoopAdd, nestedLoopIncAdd};
//     std::vector<string> nestedLoopArguments{"nestedIfBranchConst", "inputAddOp"};
//     BasicBlockPtr nestedLoopBodyBlock = make_shared<BasicBlock>("nestedLoopBlock", nestedLoopOps, nestedLoopArguments, 4);

//     OperationPtr nestedLoopHeaderCompareOp = make_shared<CompareOperation>("nestedLoopCompare", "j", "nestedIfBranchConst", CompareOperation::ISLT);
//     OperationPtr nestedLoopIfOp = make_shared<IfOperation>("loopCompare", nestedLoopBodyBlock, loopEndBlock);
//     std::vector<OperationPtr> nestedLoopHeaderBBOps{nestedLoopHeaderCompareOp, nestedLoopIfOp};
//     std::vector<string> nestedLoopHeaderArgs{"j", "nestedIfBRanchConst", "inputAddOpp"};
//     BasicBlockPtr nestedLoopHeaderBlock = make_shared<BasicBlock>("loopHeaderBlock", nestedLoopHeaderBBOps, nestedLoopHeaderArgs, 3);
//     std::vector<string> nestedLoopTerminatorArgs{"nestedLoopIncAdd", "nestedIfBRanchConst", "inputAddOpp"};
//     OperationPtr nestedLoopTerminator = make_shared<BranchOperation>(nestedLoopHeaderBlock, nestedLoopTerminatorArgs);
//     nestedLoopBodyBlock->addOperation(nestedLoopTerminator);
//     shared_ptr<LoopOperation> nestedLoopOperation = make_shared<LoopOperation>(LoopOperation::ForLoop, nestedLoopHeaderBlock);
// //==------------------------------------------------==/
//     std::vector<OperationPtr> afterNestedIfOps{nestedLoopOperation};
//     std::vector<string> afterNestedIfArgs{"inputAddOp", "nestedIfBranchConst", "j"};
//     BasicBlockPtr afterNestedIfBlock = make_shared<BasicBlock>("nestedIfEndBlock", afterNestedIfOps, afterNestedIfArgs, 3);

//     auto thenAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "const1Op");
//     OperationPtr nestedCompareOp = make_shared<CompareOperation>("nestedCompare", "inputAddOp", "const100", CompareOperation::ISLT);
//     // ThenThen
//     auto thenThenConstOp = make_shared<ConstantIntOperation>("nestedIfBranchConst", 1, 64);
//     std::vector<OperationPtr> thenThenOps{thenThenConstOp};
//     std::vector<string> thenThenArgs{"nestedIfBranchConst"};
//     OperationPtr thenThenTerminator = make_shared<BranchOperation>(afterNestedIfBlock); 
//     // ThenElse
//     auto thenElseConstOp = make_shared<ConstantIntOperation>("nestedIfBranchConst", 47, 64);
//     auto thenElseAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "const47");
//     std::vector<OperationPtr> thenElseOps{thenElseConstOp, thenElseAddOp};
//     std::vector<string> thenElseArgs{"outputDataBuffer", "inputAddOp", "nestedIfBranchConst"};
//     OperationPtr thenElseTerminator = make_shared<BranchOperation>(afterNestedIfBlock);
//     shared_ptr<IfOperation> nestedIfOp = createIfElseOperation("nestedCompare", 4, thenThenOps, thenThenArgs, thenThenTerminator, 
//                                             thenElseOps, thenElseArgs, thenElseTerminator);

//     std::vector<OperationPtr> thenBlockOps{thenAddOp, proxyPrintOp, nestedCompareOp};
//     std::vector<string> thenBlockArgs{"outputDataBuffer", "inputAddOp", "const1Op", "const100", "nestedIfBranchConst"};
//     //==--------------------------------==/

//     // Else Block Ops & Args:
//     auto elseAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "const8");
//     auto elseBranchConstOp = make_shared<ConstantIntOperation>("nestedIfBranchConst", 8, 64);
//     std::vector<OperationPtr> elseBlockOps{elseAddOp, proxyPrintOp, elseBranchConstOp};
//     std::vector<string> elseBlockArgs{"outputDataBuffer", "inputAddOp", "nestedIfBranchConst"};
//     std::vector<Operation::BasicType> elseBlockArgTypes{Operation::BasicType::INT8PTR, Operation::BasicType::INT64};
//     OperationPtr elseTerminator = make_shared<BranchOperation>(loopEndBlock);
    
//     shared_ptr<IfOperation> loopBodyIfOp = createIfElseOperation("loopCompare", 3, thenBlockOps, thenBlockArgs, 
//                                                                  nestedIfOp, elseBlockOps, elseBlockArgs, elseTerminator);

//     // Adding Terminator Operation to Loop's Body branch and to  Loop's If Operation branch.
//     loopBodyBlock->addOperation(loopBodyIfOp);

//     // Execute Function
//     std::vector<string> constNames{"i", "const1Op", "const50", "const47", "const8", "const100", "nestedIfBranchConst", "j"} ;
//     std::vector<uint64_t> constVals{0, 1, 50, 47, 8, 100, 0, 0};
//     std::vector<uint8_t> constBits{64, 64, 64, 64, 64, 64, 64, 64};
//     shared_ptr<NES::FunctionOperation> executeFunctionOp = createExecuteFunction(constNames, constVals, constBits);
//     executeFunctionOp->getFunctionBasicBlock()->addOperation(loopOperation);

//     // NESIR
//     auto nesIR = std::make_shared<NES::NESIR>(executeFunctionOp);

//     // NESIR to MLIR
//     auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
//     int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
//     assert(loadedModuleSuccess == 0);

//     mlirUtility->runJit(false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

//     // Print OutputBuffer after execution.
//     std::vector<Operation::BasicType> types{Operation::INT64};
//     auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
//     printBuffer(types, numTuples, outputBufferPointer);
// }


}// namespace NES