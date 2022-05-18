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

__attribute__((always_inline)) void printValueFromMLIR(uint64_t value) {
    printf("Add Result: %ld\n\n", value);
}

void printBuffer(vector<Operation::BasicType> types,
                 uint64_t numTuples, int8_t *bufferPointer) {
    for(uint64_t i = 0; i < numTuples; ++i) {
        printf("------------\nTuple Nr. %lu\n------------\n", i+1);
        for(auto type : types) {
            switch(type) {
                case Operation::BasicType::INT1: {
                    printf("Value(INT32): %d \n", *bufferPointer);
                    bufferPointer += 1;
                    break;
                }
                case Operation::BasicType::INT8: {
                    printf("Value(INT32): %d \n", *bufferPointer);
                    bufferPointer += 1;
                    break;
                }
                case Operation::BasicType::INT16: {
                    int16_t *value = (int16_t*) bufferPointer;
                    printf("Value(INT32): %d \n", *value);
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
                    printf("Value(INT32): %ld \n", *value);
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

BasicBlockPtr getSimpleZeroReturnBlock() {
    // Execute Return Block
    OperationPtr executeReturnOp = make_shared<ReturnOperation>(0);
    vector<OperationPtr> executeReturnBlockOps{executeReturnOp};
    vector<string> executeReturnBlockArgs{};
    return make_shared<BasicBlock>("loopBlock", executeReturnBlockOps, executeReturnBlockArgs, 1);
}

shared_ptr<FunctionOperation> createExecuteFunction(vector<string> constNames, vector<uint64_t> constVals, 
                                                    vector<uint8_t> constBits) {   
    vector<OperationPtr> executeBlockOps;
    // Loop proxy Ops
    vector<string> getInputDataBufArgs{"inputTupleBuffer"};
    vector<string> getOutputDataBufArgs{"outputTupleBuffer"};
    vector<Operation::BasicType> getOutputDataBufArgTypes{Operation::BasicType::INT8PTR};
    executeBlockOps.push_back(make_shared<ProxyCallOperation>(Operation::GetDataBuffer, "inputDataBuffer", getInputDataBufArgs, 
                                                              getOutputDataBufArgTypes, Operation::BasicType::INT8PTR));
    executeBlockOps.push_back(make_shared<ProxyCallOperation>(Operation::GetDataBuffer, "outputDataBuffer", getOutputDataBufArgs,
                                                              getOutputDataBufArgTypes, Operation::BasicType::INT8PTR));
    executeBlockOps.push_back(make_shared<ProxyCallOperation>(Operation::GetNumTuples, "numTuples", getInputDataBufArgs,
                                                              getOutputDataBufArgTypes, Operation::BasicType::INT64));

    // Loop condition constants.
    for(int i = 0; i < (int) constNames.size(); ++i) {
        executeBlockOps.push_back(make_shared<ConstantIntOperation>(constNames.at(i), constVals.at(i), constBits.at(i)));
    }
    
    vector<string> executeBodyBlockArgs{"inputTupleBuffer", "outputTupleBuffer"};
    NES::BasicBlockPtr executeBodyBlock = make_shared<NES::BasicBlock>("executeFuncBB", executeBlockOps, executeBodyBlockArgs, 0);
    vector<Operation::BasicType> executeArgTypes{ Operation::INT8PTR, Operation::INT8PTR};
    vector<string> executeArgNames{ "inputTupleBuffer", "outputTupleBuffer"};
    return make_shared<FunctionOperation>("execute", executeBodyBlock, executeArgTypes, executeArgNames, Operation::INT64);
}

BasicBlockPtr createSimpleLoopEnd(BasicBlockPtr loopBodyBlock) {
    auto loopIncAdd = make_shared<AddIntOperation>("loopIncAdd", "i", "constOne");
    OperationPtr loopBodyTerminatorOp = make_shared<BranchOperation>(loopBodyBlock);
    vector<OperationPtr> loopEndOps{loopIncAdd, loopBodyTerminatorOp};
    vector<string> loopEndArgs{"i", "constOne"};
    return make_shared<BasicBlock>("loopEndBlock", loopEndOps, loopEndArgs, 2);
}

BasicBlockPtr createSpecialLoopEnd(BasicBlockPtr loopBodyBlock, vector<OperationPtr> optionalEndOps, vector<string> optionalEndArgs) {
    auto loopIncAdd = make_shared<AddIntOperation>("loopIncAdd", "i", "constOne");
    OperationPtr loopBodyTerminatorOp = make_shared<BranchOperation>(loopBodyBlock);
    optionalEndOps.push_back(loopIncAdd);
    optionalEndOps.push_back(loopBodyTerminatorOp);
    optionalEndArgs.push_back("i");
    optionalEndArgs.push_back("constOne");
    return make_shared<BasicBlock>("loopEndBlock", optionalEndOps, optionalEndArgs, 2);
}

shared_ptr<LoopOperation> createLoopOperation(vector<string> loopHeaderArgs, BasicBlockPtr loopBodyBlock) {
    //Loop Header
    OperationPtr ifCompareOp = make_shared<CompareOperation>("loopCompare", "i", "numTuples", CompareOperation::ISLT);
    OperationPtr loopIfOp = make_shared<IfOperation>("loopCompare", loopBodyBlock, getSimpleZeroReturnBlock());
    vector<OperationPtr> loopHeaderBBOps{ifCompareOp, loopIfOp};
    BasicBlockPtr loopHeaderBlock = make_shared<BasicBlock>("loopHeaderBlock", loopHeaderBBOps, loopHeaderArgs, 1);
    // Loop Operation -> loopBranchOp -> loopHeaderBlock -> loopIfOp -> (loopBodyBlock | executeEndBlock)
    return make_shared<LoopOperation>(LoopOperation::ForLoop, loopHeaderBlock);
}

shared_ptr<IfOperation> createIfOperation(string compareArg, int blockScopeLevel, vector<OperationPtr> thenOps, 
                                          vector<string> thenArgs, OperationPtr thenTerminatorOp) {
    BasicBlockPtr loopIfThenBlock = make_shared<BasicBlock>("LoopIfThenBlock", thenOps, thenArgs, blockScopeLevel);
    loopIfThenBlock->addOperation(thenTerminatorOp);
    return make_shared<IfOperation>(compareArg, loopIfThenBlock, nullptr);
}

shared_ptr<IfOperation> createIfElseOperation(string compareArg, int blockScopeLevel, vector<OperationPtr> thenOps, 
                                              vector<string> thenArgs, OperationPtr thenTerminatorOp, 
                                              vector<OperationPtr> elseOps, vector<string> elseArgs,
                                              OperationPtr elseTerminatorOp) {
    BasicBlockPtr loopIfThenBlock = make_shared<BasicBlock>("LoopIfThenBlock", thenOps, thenArgs, blockScopeLevel);
    BasicBlockPtr loopIfElseBlock = make_shared<BasicBlock>("LoopIfElseBlock", elseOps, elseArgs, blockScopeLevel);
    loopIfThenBlock->addOperation(thenTerminatorOp);
    loopIfElseBlock->addOperation(elseTerminatorOp);
    return make_shared<IfOperation>(compareArg, loopIfThenBlock, loopIfElseBlock);
}


//==----------------------------------------------------------------------==//
//==-------------------------- IF ELSE TESTS -----------------------------==//
//==----------------------------------------------------------------------==//

//Todo
// If-Else test with loopOp after nested if-else, in Then block of parent if-else
TEST(MLIRNESIRTEST_IF, NESIRIfElseNestedMultipleFollowUps) {
    const uint64_t numTuples = 2;
    auto inAndOutputBuffer = createInAndOutputBuffers();

    //Debug Print
    vector<string> proxyArgNames{"inputAddOp"};
    vector<Operation::BasicType> proxyArgTypes{};
    auto proxyPrintOp = make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other, "printValueFromMLIR", proxyArgNames,
                                                        proxyArgTypes, Operation::BasicType::VOID);

    // Loop BodyBlock Operations
    // Todo: AddressOp should access values at "i" to get "currentRecordIdx"
    auto inputAddressOp = make_shared<AddressOperation>("inTBAddressOp", NES::Operation::BasicType::INT64, 9, 1, "i", "inputDataBuffer");
    auto loadOp = make_shared<LoadOperation>("loadTBValOp", inputAddressOp, "inTBAddressOp");
    auto addOp = make_shared<AddIntOperation>("inputAddOp", "loadTBValOp", "const47");
    

    // Loop BodyBlock up to (not including) IF Operation
    OperationPtr loopBodyCompOp = make_shared<CompareOperation>("loopCompare", "inputAddOp", "const50", CompareOperation::ISLT);
    vector<OperationPtr> loopOps{inputAddressOp, loadOp, addOp, proxyPrintOp, loopBodyCompOp}; //IfOp added later
    vector<string> loopArguments{"inputDataBuffer", "outputDataBuffer", "numTuples", "i", "constOne", "const47", "const50", 
                                 "const100", "const8", "nestedIfBranchConst"};
    BasicBlockPtr loopBodyBlock = make_shared<BasicBlock>("loopBlock", loopOps, loopArguments, 2);

    // Create Loop Operation
    vector<string> loopHeaderArgs{"inputDataBuffer", "outputDataBuffer", "i", "constOne", "const47", "const50", "const100", 
                                  "const8", "numTuples", "nestedIfBranchConst"};
    shared_ptr<LoopOperation> loopOperation = createLoopOperation(loopHeaderArgs, loopBodyBlock);

    auto loopEndAdd = make_shared<AddIntOperation>("loopEndAdd", "inputAddOp", "nestedIfBranchConst");
    auto outputAddressOp = make_shared<AddressOperation>("outTBAddressOp", NES::Operation::BasicType::INT64, 8, 0, "i", "outputDataBuffer");
    auto storeOp = make_shared<StoreOperation>("loopEndAdd", "outTBAddressOp");
    auto storeOp2 = make_shared<StoreOperation>("nestedIfBranchConst", "outTBAddressOp");
    vector<OperationPtr> loopEndOps{loopEndAdd, outputAddressOp, storeOp, storeOp2};
    vector<string> loopEndArgs{"inputAddOp", "nestedIfBranchConst", "outputDataBuffer"};
    BasicBlockPtr loopEndBlock = createSpecialLoopEnd(loopOperation->getLoopHeaderBlock(), loopEndOps, loopEndArgs);

    // Create If-only If operation
    // Then Block Ops & Args:
    //==--------- NESTED IF ------------==/
//==--------- NESTED LOOP OPERATION!!!! ------------==/
    auto nestedLoopAdd = make_shared<AddIntOperation>("nestedLoopAdd", "inputAddOp", "nestedIfBranchConst");
    auto nestedLoopIncAdd = make_shared<AddIntOperation>("nestedLoopIncAdd", "j", "constOne");

    vector<OperationPtr> nestedLoopOps{nestedLoopAdd, nestedLoopIncAdd};
    vector<string> nestedLoopArguments{"nestedIfBranchConst", "inputAddOp"};
    BasicBlockPtr nestedLoopBodyBlock = make_shared<BasicBlock>("nestedLoopBlock", nestedLoopOps, nestedLoopArguments, 4);

    OperationPtr nestedLoopHeaderCompareOp = make_shared<CompareOperation>("nestedLoopCompare", "j", "nestedIfBranchConst", CompareOperation::ISLT);
    OperationPtr nestedLoopIfOp = make_shared<IfOperation>("loopCompare", nestedLoopBodyBlock, loopEndBlock);
    vector<OperationPtr> nestedLoopHeaderBBOps{nestedLoopHeaderCompareOp, nestedLoopIfOp};
    vector<string> nestedLoopHeaderArgs{"j", "nestedIfBRanchConst", "inputAddOpp"};
    BasicBlockPtr nestedLoopHeaderBlock = make_shared<BasicBlock>("loopHeaderBlock", nestedLoopHeaderBBOps, nestedLoopHeaderArgs, 3);
    OperationPtr nestedLoopTerminator = make_shared<BranchOperation>(nestedLoopHeaderBlock);
    nestedLoopBodyBlock->addOperation(nestedLoopTerminator);
    shared_ptr<LoopOperation> nestedLoopOperation = make_shared<LoopOperation>(LoopOperation::ForLoop, nestedLoopHeaderBlock);
//==------------------------------------------------==/
    vector<OperationPtr> afterNestedIfOps{nestedLoopOperation};
    vector<string> afterNestedIfArgs{"inputAddOp", "nestedIfBranchConst", "j"};
    BasicBlockPtr afterNestedIfBlock = make_shared<BasicBlock>("nestedIfEndBlock", afterNestedIfOps, afterNestedIfArgs, 3);

    auto thenAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "constOne");
    OperationPtr nestedCompareOp = make_shared<CompareOperation>("nestedCompare", "inputAddOp", "const100", CompareOperation::ISLT);
    // ThenThen
    auto thenThenConstOp = make_shared<ConstantIntOperation>("nestedIfBranchConst", 1, 64);
    vector<OperationPtr> thenThenOps{thenThenConstOp};
    vector<string> thenThenArgs{"nestedIfBranchConst"};
    OperationPtr thenThenTerminator = make_shared<BranchOperation>(afterNestedIfBlock); 
    // ThenElse
    auto thenElseConstOp = make_shared<ConstantIntOperation>("nestedIfBranchConst", 47, 64);
    auto thenElseAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "const47");
    std::vector<OperationPtr> thenElseOps{thenElseConstOp, thenElseAddOp};
    vector<string> thenElseArgs{"outputDataBuffer", "inputAddOp", "nestedIfBranchConst"};
    OperationPtr thenElseTerminator = make_shared<BranchOperation>(afterNestedIfBlock);
    shared_ptr<IfOperation> nestedIfOp = createIfElseOperation("nestedCompare", 4, thenThenOps, thenThenArgs, thenThenTerminator, 
                                            thenElseOps, thenElseArgs, thenElseTerminator);

    vector<OperationPtr> thenBlockOps{thenAddOp, proxyPrintOp, nestedCompareOp};
    vector<string> thenBlockArgs{"outputDataBuffer", "inputAddOp", "constOne", "const100", "nestedIfBranchConst"};
    //==--------------------------------==/

    // Else Block Ops & Args:
    auto elseAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "const8");
    auto elseBranchConstOp = make_shared<ConstantIntOperation>("nestedIfBranchConst", 8, 64);
    vector<OperationPtr> elseBlockOps{elseAddOp, proxyPrintOp, elseBranchConstOp};
    vector<string> elseBlockArgs{"outputDataBuffer", "inputAddOp", "nestedIfBranchConst"};
    vector<Operation::BasicType> elseBlockArgTypes{Operation::BasicType::INT8PTR, Operation::BasicType::INT64};
    OperationPtr elseTerminator = make_shared<BranchOperation>(loopEndBlock);
    
    shared_ptr<IfOperation> loopBodyIfOp = createIfElseOperation("loopCompare", 3, thenBlockOps, thenBlockArgs, 
                                                                 nestedIfOp, elseBlockOps, elseBlockArgs, elseTerminator);

    // Adding Terminator Operation to Loop's Body branch and to  Loop's If Operation branch.
    loopBodyBlock->addOperation(loopBodyIfOp);

    // Execute Function
    vector<string> constNames{"i", "constOne", "const50", "const47", "const8", "const100", "nestedIfBranchConst", "j"} ;
    vector<uint64_t> constVals{0, 1, 50, 47, 8, 100, 0, 0};
    vector<uint8_t> constBits{64, 64, 64, 64, 64, 64, 64, 64};
    shared_ptr<NES::FunctionOperation> executeFunctionOp = createExecuteFunction(constNames, constVals, constBits);
    executeFunctionOp->getFunctionBasicBlock()->addOperation(loopOperation);

    // NESIR
    NES::NESIR nesIR(executeFunctionOp);

    // NESIR to MLIR
    auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(&nesIR);
    assert(loadedModuleSuccess == 0);

    // Register buffers and functions with JIT and run module.
    const vector<string> symbolNames{"printValueFromMLIR"};
    // Order must match symbolNames order!
    const vector<llvm::JITTargetAddress> jitAddresses{
            llvm::pointerToJITTargetAddress(&printValueFromMLIR),
    };
    mlirUtility->runJit(symbolNames, jitAddresses, false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

    // Print OutputBuffer after execution.
    vector<Operation::BasicType> types{Operation::INT64};
    auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}

// TEST(DISABLED_MLIRNESIRTEST_IF, NESIRIfElseNestedMultipleFollowUps) {
//     const uint64_t numTuples = 2;
//     auto inAndOutputBuffer = createInAndOutputBuffers();

//     // Loop BodyBlock Operations
//     // Todo: AddressOp should access values at "i" to get "currentRecordIdx"
//     auto inputAddressOp = make_shared<AddressOperation>("inTBAddressOp", NES::Operation::BasicType::INT64, 9, 1, "inputDataBuffer");
//     auto loadOp = make_shared<LoadOperation>("loadTBValOp", inputAddressOp, "inTBAddressOp");
//     auto addOp = make_shared<AddIntOperation>("inputAddOp", "loadTBValOp", "const47");

//     // Loop BodyBlock up to (not including) IF Operation
//     OperationPtr loopBodyCompOp = make_shared<CompareOperation>("loopCompare", "inputAddOp", "const50", CompareOperation::ISLT);
//     vector<OperationPtr> loopOps{inputAddressOp, loadOp, addOp, loopBodyCompOp}; //IfOp added later
//     vector<string> loopArguments{"inputDataBuffer", "outputDataBuffer", "numTuples", "i", "constOne", "const47", "const50", 
//                                  "const100", "const8", "nestedIfBranchConst"};
//     BasicBlockPtr loopBodyBlock = make_shared<BasicBlock>("loopBlock", loopOps, loopArguments, 2);

//     // Create Loop Operation
//     vector<string> loopHeaderArgs{"inputDataBuffer", "outputDataBuffer", "i", "constOne", "const47", "const50", "const100", 
//                                   "const8", "numTuples", "nestedIfBranchConst"};
//     shared_ptr<LoopOperation> loopOperation = createLoopOperation(loopHeaderArgs, loopBodyBlock);

//     auto loopEndAdd = make_shared<AddIntOperation>("loopEndAdd", "inputAddOp", "nestedIfBranchConst");
//     auto outputAddressOp = make_shared<AddressOperation>("outTBAddressOp", NES::Operation::BasicType::INT64, 8, 0, "outputDataBuffer");
//     auto storeOp = make_shared<StoreOperation>("loopEndAdd", "outTBAddressOp");
//     auto storeOp2 = make_shared<StoreOperation>("nestedIfBranchConst", "outTBAddressOp");
//     vector<OperationPtr> loopEndOps{loopEndAdd, outputAddressOp, storeOp, storeOp2};
//     vector<string> loopEndArgs{"inputAddOp", "nestedIfBranchConst", "outputDataBuffer"};
//     //Todo change from loopBodyBlock -> loopHeaderBlock
//     BasicBlockPtr loopEndBlock = createSpecialLoopEnd(loopOperation->getLoopHeaderBlock(), loopEndOps, loopEndArgs);

//     // Create If-only If operation
//     // Then Block Ops & Args:
//     //==--------- NESTED IF ------------==/
//     auto afterNestedIfAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "nestedIfBranchConst");
//     OperationPtr nestedIfTerminator = make_shared<BranchOperation>(loopEndBlock);
//     vector<OperationPtr> afterNestedIfOps{afterNestedIfAddOp, nestedIfTerminator};
//     vector<string> afterNestedIfArgs{"inputAddOp", "nestedIfBranchConst"};
//     BasicBlockPtr afterNestedIfBlock = make_shared<BasicBlock>("nestedIfEndBlock", afterNestedIfOps, afterNestedIfArgs, 3);

//     auto thenAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "constOne");
//     OperationPtr nestedCompareOp = make_shared<CompareOperation>("nestedCompare", "inputAddOp", "const100", CompareOperation::ISLT);
//     // ThenThen
//     auto thenThenConstOp = make_shared<ConstantIntOperation>("nestedIfBranchConst", 1, 64);
//     vector<OperationPtr> thenThenOps{thenThenConstOp};
//     vector<string> thenThenArgs{"nestedIfBranchConst"};
//     OperationPtr thenThenTerminator = make_shared<BranchOperation>(afterNestedIfBlock); 
//     // ThenElse
//     auto thenElseConstOp = make_shared<ConstantIntOperation>("nestedIfBranchConst", 47, 64);
//     auto thenElseAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "const47");
//     std::vector<OperationPtr> thenElseOps{thenElseConstOp, thenElseAddOp};
//     vector<string> thenElseArgs{"outputDataBuffer", "inputAddOp", "nestedIfBranchConst"};
//     OperationPtr thenElseTerminator = make_shared<BranchOperation>(afterNestedIfBlock);
//     shared_ptr<IfOperation> nestedIfOp = createIfElseOperation("nestedCompare", 4, thenThenOps, thenThenArgs, thenThenTerminator, 
//                                             thenElseOps, thenElseArgs, thenElseTerminator);

//     vector<OperationPtr> thenBlockOps{thenAddOp, nestedCompareOp};
//     vector<string> thenBlockArgs{"outputDataBuffer", "inputAddOp", "constOne", "const100", "nestedIfBranchConst"};
//     //==--------------------------------==/

//     // Else Block Ops & Args:
//     auto elseAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "const8");
//     auto elseBranchConstOp = make_shared<ConstantIntOperation>("nestedIfBranchConst", 8, 64);
//     vector<OperationPtr> elseBlockOps{elseAddOp, elseBranchConstOp};
//     vector<string> elseBlockArgs{"outputDataBuffer", "inputAddOp", "nestedIfBranchConst"};
//     vector<Operation::BasicType> elseBlockArgTypes{Operation::BasicType::INT8PTR, Operation::BasicType::INT64};
//     OperationPtr elseTerminator = make_shared<BranchOperation>(loopEndBlock);
    
//     shared_ptr<IfOperation> loopBodyIfOp = createIfElseOperation("loopCompare", 3, thenBlockOps, thenBlockArgs, 
//                                                                  nestedIfOp, elseBlockOps, elseBlockArgs, elseTerminator);

//     // Adding Terminator Operation to Loop's Body branch and to  Loop's If Operation branch.
//     loopBodyBlock->addOperation(loopBodyIfOp);

//     // Execute Function
//     vector<string> constNames{"i", "constOne", "const50", "const47", "const8", "const100", "nestedIfBranchConst"} ;
//     vector<uint64_t> constVals{0, 1, 50, 47, 8, 100, 0};
//     vector<uint8_t> constBits{64, 64, 64, 64, 64, 64, 64};
//     shared_ptr<NES::FunctionOperation> executeFunctionOp = createExecuteFunction(constNames, constVals, constBits);
//     executeFunctionOp->getFunctionBasicBlock()->addOperation(loopOperation);

//     // NESIR
//     NES::NESIR nesIR(executeFunctionOp);

//     // NESIR to MLIR
//     auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
//     int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(&nesIR);
//     assert(loadedModuleSuccess == 0);

//     // Register buffers and functions with JIT and run module.
//     const vector<string> symbolNames{"printValueFromMLIR"};
//     // Order must match symbolNames order!
//     const vector<llvm::JITTargetAddress> jitAddresses{
//             llvm::pointerToJITTargetAddress(&printValueFromMLIR),
//     };
//     mlirUtility->runJit(symbolNames, jitAddresses, false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

//     // Print OutputBuffer after execution.
//     vector<Operation::BasicType> types{Operation::INT64};
//     auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
//     printBuffer(types, numTuples, outputBufferPointer);
// }

// TEST(DISABLED_MLIRNESIRTEST_IF, NESIRIfElseNestedFollowUp) {
//     const uint64_t numTuples = 2;
//     auto inAndOutputBuffer = createInAndOutputBuffers();

//     // Loop BodyBlock Operations
//     // Todo: AddressOp should access values at "i" to get "currentRecordIdx"
//     auto inputAddressOp = make_shared<AddressOperation>("inTBAddressOp", NES::Operation::BasicType::INT64, 9, 1, "inputDataBuffer");
//     auto loadOp = make_shared<LoadOperation>("loadTBValOp", inputAddressOp, "inTBAddressOp");
//     auto addOp = make_shared<AddIntOperation>("inputAddOp", "loadTBValOp", "const47");

//     // Loop BodyBlock up to (not including) IF Operation
//     OperationPtr loopBodyCompOp = make_shared<CompareOperation>("loopCompare", "inputAddOp", "const50", CompareOperation::ISLT);
//     vector<OperationPtr> loopOps{inputAddressOp, loadOp, addOp, loopBodyCompOp}; //IfOp added later
//     vector<string> loopArguments{"inputDataBuffer", "outputDataBuffer", "numTuples", "i", "constOne", "const47", "const50", 
//                                  "const100", "const8", "nestedIfBranchConst"};
//     BasicBlockPtr loopBodyBlock = make_shared<BasicBlock>("loopBlock", loopOps, loopArguments, 2);

//     // Create Loop Operation
//     vector<string> loopHeaderArgs{"inputDataBuffer", "outputDataBuffer", "i", "constOne", "const47", "const50", "const100", 
//                                   "const8", "numTuples", "nestedIfBranchConst"};
//     shared_ptr<LoopOperation> loopOperation = createLoopOperation(loopHeaderArgs, loopBodyBlock);

//     auto loopEndAdd = make_shared<AddIntOperation>("loopEndAdd", "inputAddOp", "nestedIfBranchConst");
//     auto outputAddressOp = make_shared<AddressOperation>("outTBAddressOp", NES::Operation::BasicType::INT64, 8, 0, "outputDataBuffer");
//     auto storeOp = make_shared<StoreOperation>("loopEndAdd", "outTBAddressOp");
//     auto storeOp2 = make_shared<StoreOperation>("nestedIfBranchConst", "outTBAddressOp");
//     vector<OperationPtr> loopEndOps{loopEndAdd, outputAddressOp, storeOp, storeOp2};
//     vector<string> loopEndArgs{"inputAddOp", "nestedIfBranchConst", "outputDataBuffer"};
//     BasicBlockPtr loopEndBlock = createSpecialLoopEnd(loopOperation->getLoopHeaderBlock(), loopEndOps, loopEndArgs);

//     // Create If-only If operation
//     // Then Block Ops & Args:
//     //Todo 
//     //==--------- NESTED IF ------------==/
//     auto thenAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "constOne");
//     OperationPtr nestedCompareOp = make_shared<CompareOperation>("nestedCompare", "inputAddOp", "const100", CompareOperation::ISLT);
//     // ThenThen
//     auto thenThenConstOp = make_shared<ConstantIntOperation>("nestedIfBranchConst", 1, 64);
//     vector<OperationPtr> thenThenOps{thenThenConstOp};
//     vector<string> thenThenArgs{"nestedIfBranchConst"};
//     OperationPtr thenThenTerminator = make_shared<BranchOperation>(loopEndBlock);
//     // ThenElse
//     auto thenElseConstOp = make_shared<ConstantIntOperation>("nestedIfBranchConst", 47, 64); //Todo can we use inputAddOp here?
//     auto thenElseAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "const47");
//     std::vector<OperationPtr> thenElseOps{thenElseConstOp, thenElseAddOp};
//     vector<string> thenElseArgs{"outputDataBuffer", "inputAddOp", "nestedIfBranchConst"};
//     OperationPtr thenElseTerminator = make_shared<BranchOperation>(loopEndBlock);
//     shared_ptr<IfOperation> nestedIfOp = createIfElseOperation("nestedCompare", 4, thenThenOps, thenThenArgs, thenThenTerminator, 
//                                             thenElseOps, thenElseArgs, thenElseTerminator);

//     vector<OperationPtr> thenBlockOps{thenAddOp, nestedCompareOp, nestedIfOp};
//     vector<string> thenBlockArgs{"outputDataBuffer", "inputAddOp", "constOne", "const100", "nestedIfBranchConst"};
//     OperationPtr thenTerminator = make_shared<BranchOperation>(loopEndBlock);
//     //==--------------------------------==/

//     // Else Block Ops & Args:
//     auto elseAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "const8");
//     auto elseBranchConstOp = make_shared<ConstantIntOperation>("nestedIfBranchConst", 8, 64);
//     vector<OperationPtr> elseBlockOps{elseAddOp, elseBranchConstOp};
//     vector<string> elseBlockArgs{"outputDataBuffer", "inputAddOp", "nestedIfBranchConst"};
//     vector<Operation::BasicType> elseBlockArgTypes{Operation::BasicType::INT8PTR, Operation::BasicType::INT64};
//     OperationPtr elseTerminator = make_shared<BranchOperation>(loopEndBlock);
    
//     shared_ptr<IfOperation> loopBodyIfOp = createIfElseOperation("loopCompare", 3, thenBlockOps, thenBlockArgs, 
//                                                                  thenTerminator, elseBlockOps, elseBlockArgs, elseTerminator);

//     // Adding Terminator Operation to Loop's Body branch and to  Loop's If Operation branch.
//     loopBodyBlock->addOperation(loopBodyIfOp);

//     // Execute Function
//     vector<string> constNames{"i", "constOne", "const50", "const47", "const8", "const100", "nestedIfBranchConst"} ;
//     vector<uint64_t> constVals{0, 1, 50, 47, 8, 100, 0};
//     vector<uint8_t> constBits{64, 64, 64, 64, 64, 64, 64};
//     shared_ptr<NES::FunctionOperation> executeFunctionOp = createExecuteFunction(constNames, constVals, constBits);
//     executeFunctionOp->getFunctionBasicBlock()->addOperation(loopOperation);

//     // NESIR
//     NES::NESIR nesIR(executeFunctionOp);

//     // NESIR to MLIR
//     auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
//     int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(&nesIR);
//     assert(loadedModuleSuccess == 0);

//     // Register buffers and functions with JIT and run module.
//     const vector<string> symbolNames{"printValueFromMLIR"};
//     // Order must match symbolNames order!
//     const vector<llvm::JITTargetAddress> jitAddresses{
//             llvm::pointerToJITTargetAddress(&printValueFromMLIR),
//     };
//     mlirUtility->runJit(symbolNames, jitAddresses, false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

//     // Print OutputBuffer after execution.
//     vector<Operation::BasicType> types{Operation::INT64};
//     auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
//     printBuffer(types, numTuples, outputBufferPointer);
// }

// TEST(DISABLED_MLIRNESIRTEST_IF, simpleNESIRIFtest) {
//     const uint64_t numTuples = 2;
//     auto inAndOutputBuffer = createInAndOutputBuffers();

//     // Loop BodyBlock Operations
//     // Todo: AddressOp should access values at "i" to get "currentRecordIdx"
//     auto inputAddressOp = make_shared<AddressOperation>("inTBAddressOp", NES::Operation::BasicType::INT64, 9, 1, "inputDataBuffer");
//     auto loadOp = make_shared<LoadOperation>("loadTBValOp", inputAddressOp, "inTBAddressOp");
//     auto addOp = make_shared<AddIntOperation>("inputAddOp", "loadTBValOp", "const42");

//     // Loop BodyBlock up to (not including) IF Operation
//     OperationPtr loopBodyCompOp = make_shared<CompareOperation>("loopCompare", "inputAddOp", "const50", CompareOperation::ISLT);
//     vector<OperationPtr> loopOps{inputAddressOp, loadOp, addOp, loopBodyCompOp}; //IfOp added later
//     vector<string> loopArguments{"inputDataBuffer", "outputDataBuffer", "numTuples", "i", "constOne", "const42", "const50"};
//     BasicBlockPtr loopBodyBlock = make_shared<BasicBlock>("loopBlock", loopOps, loopArguments, 2);

//     // Create Loop Operation
//     vector<string> loopHeaderArgs{"inputDataBuffer", "outputDataBuffer", "i", "constOne", "const42", "const50", "const8", "numTuples"};
//     shared_ptr<LoopOperation> loopOperation = createLoopOperation(loopHeaderArgs, loopBodyBlock);
//     BasicBlockPtr loopEndBlock = createSimpleLoopEnd(loopBodyBlock);

//     // Create If-only If operation
//     auto outputAddressOp = make_shared<AddressOperation>("outTBAddressOp", NES::Operation::BasicType::INT64, 8, 0, "outputDataBuffer");
//     auto storeOp = make_shared<StoreOperation>("inputAddOp", "outTBAddressOp");
//     vector<OperationPtr> ifThenBlockOps{outputAddressOp, storeOp};
//     vector<string> loopIfThenBlockArgs{"outputDataBuffer", "inputAddOp"};
//     OperationPtr loopIfThenTerminator = make_shared<BranchOperation>(loopEndBlock);
//     shared_ptr<IfOperation> loopBodyIfOp = createIfOperation("loopCompare", 3, ifThenBlockOps, loopIfThenBlockArgs, loopIfThenTerminator);

//     // Adding Terminator Operation to Loop's Body branch and to  Loop's If Operation branch.
//     loopBodyBlock->addOperation(loopBodyIfOp);

//     // Execute Function
//     vector<string> constNames{"i", "constOne", "const50", "const42"} ;
//     vector<uint64_t> constVals{0, 1, 45, 42};
//     vector<uint8_t> constBits{64, 64, 64, 64};
//     shared_ptr<NES::FunctionOperation> executeFunctionOp = createExecuteFunction(constNames, constVals, constBits);
//     executeFunctionOp->getFunctionBasicBlock()->addOperation(loopOperation);

//     // NESIR
//     NES::NESIR nesIR(executeFunctionOp);

//     // NESIR to MLIR
//     auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
//     int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(&nesIR);
//     assert(loadedModuleSuccess == 0);

//     // Register buffers and functions with JIT and run module.
//     const vector<string> symbolNames{"printValueFromMLIR"};
//     // Order must match symbolNames order!
//     const vector<llvm::JITTargetAddress> jitAddresses{
//             llvm::pointerToJITTargetAddress(&printValueFromMLIR),
//     };
//     mlirUtility->runJit(symbolNames, jitAddresses, false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

//     // Print OutputBuffer after execution.
//     vector<Operation::BasicType> types{Operation::INT64};
//     auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
//     printBuffer(types, numTuples, outputBufferPointer);
// }

// TEST(DISABLED_MLIRNESIRTEST_IF, simpleNESIRIfElse) {
//     const uint64_t numTuples = 2;
//     auto inAndOutputBuffer = createInAndOutputBuffers();

//     // Loop BodyBlock Operations
//     // Todo: AddressOp should access values at "i" to get "currentRecordIdx"
//     auto inputAddressOp = make_shared<AddressOperation>("inTBAddressOp", NES::Operation::BasicType::INT64, 9, 1, "inputDataBuffer");
//     auto loadOp = make_shared<LoadOperation>("loadTBValOp", inputAddressOp, "inTBAddressOp");
//     auto addOp = make_shared<AddIntOperation>("inputAddOp", "loadTBValOp", "const42");
//     auto outputAddressOp = make_shared<AddressOperation>("outTBAddressOp", NES::Operation::BasicType::INT64, 8, 0, "outputDataBuffer");

//     // Loop BodyBlock up to (not including) IF Operation
//     OperationPtr loopBodyCompOp = make_shared<CompareOperation>("loopCompare", "inputAddOp", "const50", CompareOperation::ISLT);
//     vector<OperationPtr> loopOps{inputAddressOp, loadOp, addOp, loopBodyCompOp, outputAddressOp}; //IfOp added later
//     vector<string> loopArguments{"inputDataBuffer", "outputDataBuffer", "numTuples", "i", "constOne", "const42", "const50", "const8"};
//     BasicBlockPtr loopBodyBlock = make_shared<BasicBlock>("loopBlock", loopOps, loopArguments, 1);

//     // Create Loop Operation
//     vector<string> loopHeaderArgs{"inputDataBuffer", "outputDataBuffer", "i", "constOne", "const42", "const50", "const8", "numTuples"};
//     shared_ptr<LoopOperation> loopOperation = createLoopOperation(loopHeaderArgs, loopBodyBlock);
//     BasicBlockPtr loopEndBlock  = createSimpleLoopEnd(loopBodyBlock);

//     // Create If-only If operation
//     // Then Block Ops & Args:
//     auto thenStoreOp = make_shared<StoreOperation>("inputAddOp", "outTBAddressOp");
//     vector<OperationPtr> thenBlockOps{thenStoreOp};
//     vector<string> thenBlockArgs{"outputDataBuffer", "inputAddOp"};
//     OperationPtr thenTerminator = make_shared<BranchOperation>(loopEndBlock);
//     // Else Block Ops & Args:
//     auto elseAddOp = make_shared<AddIntOperation>("elseAddOp", "loadTBValOp", "const8");
//     auto elseStoreOp = make_shared<StoreOperation>("elseAddOp", "outTBAddressOp");
//     vector<OperationPtr> elseBlockOps{elseAddOp, elseStoreOp};
//     vector<string> elseBlockArgs{"outputDataBuffer", "inputAddOp"};
//     OperationPtr elseTerminator = make_shared<BranchOperation>(loopEndBlock);
    
//     shared_ptr<IfOperation> loopBodyIfOp = createIfElseOperation("loopCompare", 2, thenBlockOps, thenBlockArgs,
//                                                                  thenTerminator, elseBlockOps, elseBlockArgs, elseTerminator);

//     // Adding Terminator Operation to Loop's Body branch and to  Loop's If Operation branch.
//     loopBodyBlock->addOperation(loopBodyIfOp);

//     // Execute Function
//     vector<string> constNames{"i", "constOne", "const50", "const42", "const8"} ;
//     vector<uint64_t> constVals{0, 1, 46, 42, 8};
//     vector<uint8_t> constBits{64, 64, 64, 64, 64};
//     shared_ptr<NES::FunctionOperation> executeFunctionOp = createExecuteFunction(constNames, constVals, constBits);
//     executeFunctionOp->getFunctionBasicBlock()->addOperation(loopOperation);

//     // NESIR
//     NES::NESIR nesIR(executeFunctionOp);

//     // NESIR to MLIR
//     auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
//     int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(&nesIR);
//     assert(loadedModuleSuccess == 0);

//     // Register buffers and functions with JIT and run module.
//     const vector<string> symbolNames{"printValueFromMLIR"};
//     // Order must match symbolNames order!
//     const vector<llvm::JITTargetAddress> jitAddresses{
//             llvm::pointerToJITTargetAddress(&printValueFromMLIR),
//     };
//     mlirUtility->runJit(symbolNames, jitAddresses, false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

//     // Print OutputBuffer after execution.
//     vector<Operation::BasicType> types{Operation::INT64};
//     auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
//     printBuffer(types, numTuples, outputBufferPointer);
// }

// TEST(DISABLED_MLIRNESIRTEST_IF, simpleNESIRIfElseFollowUp) {
//     const uint64_t numTuples = 2;
//     auto inAndOutputBuffer = createInAndOutputBuffers();

//     // Loop BodyBlock Operations
//     // Todo: AddressOp should access values at "i" to get "currentRecordIdx"
//     auto inputAddressOp = make_shared<AddressOperation>("inTBAddressOp", NES::Operation::BasicType::INT64, 9, 1, "inputDataBuffer");
//     auto loadOp = make_shared<LoadOperation>("loadTBValOp", inputAddressOp, "inTBAddressOp");
//     auto addOp = make_shared<AddIntOperation>("inputAddOp", "loadTBValOp", "const47");

//     // Loop BodyBlock up to (not including) IF Operation
//     OperationPtr loopBodyCompOp = make_shared<CompareOperation>("loopCompare", "inputAddOp", "const50", CompareOperation::ISLT);
//     vector<OperationPtr> loopOps{inputAddressOp, loadOp, addOp, loopBodyCompOp}; //IfOp added later
//     vector<string> loopArguments{"inputDataBuffer", "outputDataBuffer", "numTuples", "i", "constOne", "const47", "const50", "const8"};
//     BasicBlockPtr loopBodyBlock = make_shared<BasicBlock>("loopBlock", loopOps, loopArguments, 2);

//     // Create Loop Operation
//     vector<string> loopHeaderArgs{"inputDataBuffer", "outputDataBuffer", "i", "constOne", "const47", "const50", "const8", "numTuples"};
//     shared_ptr<LoopOperation> loopOperation = createLoopOperation(loopHeaderArgs, loopBodyBlock);

//     auto outputAddressOp = make_shared<AddressOperation>("outTBAddressOp", NES::Operation::BasicType::INT64, 8, 0, "outputDataBuffer");
//     auto storeOp = make_shared<StoreOperation>("inputAddOp", "outTBAddressOp");
//     vector<OperationPtr> loopEndOps{outputAddressOp, storeOp};
//     vector<string> loopEndArgs{"inputAddOp", "outputDataBuffer"};
//     BasicBlockPtr loopEndBlock = createSpecialLoopEnd(loopOperation->getLoopHeaderBlock(), loopEndOps, loopEndArgs);

//     // Create If-only If operation
//     // Then Block Ops & Args:
//     auto thenAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "const50");
//     vector<OperationPtr> thenBlockOps{thenAddOp};
//     vector<string> thenBlockArgs{"outputDataBuffer", "inputAddOp"};
//     OperationPtr thenTerminator = make_shared<BranchOperation>(loopEndBlock);
//     // Else Block Ops & Args:
//     auto elseAddOp = make_shared<AddIntOperation>("inputAddOp", "inputAddOp", "const8");
//     vector<OperationPtr> elseBlockOps{elseAddOp};
//     vector<string> elseBlockArgs{"outputDataBuffer", "inputAddOp"};
//     OperationPtr elseTerminator = make_shared<BranchOperation>(loopEndBlock);
    
//     shared_ptr<IfOperation> loopBodyIfOp = createIfElseOperation("loopCompare", 3, thenBlockOps, thenBlockArgs, thenTerminator, 
//                                                                  elseBlockOps, elseBlockArgs, elseTerminator);
//     //Todo add storeOp to loopEndBlock
//     // -> new createLoopEnd?

//     // Adding Terminator Operation to Loop's Body branch and to  Loop's If Operation branch.
//     loopBodyBlock->addOperation(loopBodyIfOp);

//     // Execute Function
//     vector<string> constNames{"i", "constOne", "const50", "const47", "const8"} ;
//     vector<uint64_t> constVals{0, 1, 50, 47, 8};
//     vector<uint8_t> constBits{64, 64, 64, 64, 64};
//     shared_ptr<NES::FunctionOperation> executeFunctionOp = createExecuteFunction(constNames, constVals, constBits);
//     executeFunctionOp->getFunctionBasicBlock()->addOperation(loopOperation);

//     // NESIR
//     NES::NESIR nesIR(executeFunctionOp);

//     // NESIR to MLIR
//     auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
//     int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(&nesIR);
//     assert(loadedModuleSuccess == 0);

//     // Register buffers and functions with JIT and run module.
//     const vector<string> symbolNames{"printValueFromMLIR"};
//     // Order must match symbolNames order!
//     const vector<llvm::JITTargetAddress> jitAddresses{
//             llvm::pointerToJITTargetAddress(&printValueFromMLIR),
//     };
//     mlirUtility->runJit(symbolNames, jitAddresses, false, addressof(inAndOutputBuffer.first), addressof(inAndOutputBuffer.second));

//     // Print OutputBuffer after execution.
//     vector<Operation::BasicType> types{Operation::INT64};
//     auto outputBufferPointer = inAndOutputBuffer.second.getBuffer<int8_t>();
//     printBuffer(types, numTuples, outputBufferPointer);
// }



}// namespace NES