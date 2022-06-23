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

// #include <Util/Logger/Logger.hpp>
// #include <cstdint>
// #include <gtest/gtest.h>

// #include "mlir/IR/AsmState.h"
// #include "mlir/Pass/PassManager.h"
// #include "mlir/Transforms/DialectConversion.h"
// #include "llvm/ExecutionEngine/JITSymbol.h"

// #include <Experimental/MLIR/MLIRUtility.hpp>
// #include <Runtime/BufferManager.hpp>
// #include <Runtime/TupleBuffer.hpp>

// #include <Experimental/NESIR/Operations/FunctionOperation.hpp>
// #include <Experimental/NESIR/Operations/LoopOperation.hpp>
// #include <Experimental/NESIR/Operations/AddressOperation.hpp>
// #include <Experimental/NESIR/Operations/ArithmeticOperations/AddIntOperation.hpp>
// #include <Experimental/NESIR/Operations/ConstantIntOperation.hpp>
// #include <Experimental/NESIR/Operations/LoadOperation.hpp>
// #include <Experimental/NESIR/Operations/StoreOperation.hpp>
// #include <Experimental/NESIR/Operations/BranchOperation.hpp>

// #include <Experimental/NESIR/Operations/IfOperation.hpp>

// #include <Experimental/NESIR/Operations/CompareOperation.hpp>
// #include <Experimental/NESIR/Operations/ReturnOperation.hpp>
// #include "Experimental/NESIR/Operations/ProxyCallOperation.hpp"
// #include <unordered_map>

// namespace NES {
// class MLIRNESIRTest : public testing::Test {
//   public:
//     static void SetUpTestCase() {
//         NES::Logger::setupLogging("MLIRNESIRTest.log", NES::LogLevel::LOG_DEBUG);

//         NES_INFO("MLIRNESIRTest test class SetUpTestCase.");
//     }
//     static void TearDownTestCase() { NES_INFO("MLIRNESIRTest test class TearDownTestCase."); }
// };

// void printBuffer(std::vector<BasicType> types,
//                  uint64_t numTuples, int8_t *bufferPointer) {
//     for(uint64_t i = 0; i < numTuples; ++i) {
//         printf("------------\nTuple Nr. %lu\n------------\n", i+1);
//         for(auto type : types) {
//             switch(type) {
//                 case BasicType::INT1: {
//                     printf("Value(INT32): %d \n", *bufferPointer);
//                     bufferPointer += 1;
//                     break;
//                 }
//                 case BasicType::INT8: {
//                     printf("Value(INT32): %d \n", *bufferPointer);
//                     bufferPointer += 1;
//                     break;
//                 }
//                 case BasicType::INT16: {
//                     int16_t *value = (int16_t*) bufferPointer;
//                     printf("Value(INT32): %d \n", *value);
//                     bufferPointer += 2;
//                     break;
//                 }
//                 case BasicType::INT32: {
//                     int32_t *value = (int32_t*) bufferPointer;
//                     printf("Value(INT32): %d \n", *value);
//                     bufferPointer += 4;
//                     break;
//                 }
//                 case BasicType::INT64: {
//                     int64_t *value = (int64_t*) bufferPointer;
//                     printf("Value(INT32): %ld \n", *value);
//                     bufferPointer += 8;
//                     break;
//                 }
//                 case BasicType::BOOLEAN: {
//                     bool *value = (bool*) bufferPointer;
//                     printf("Value(BOOL): %s \n", (*value) ? "true" : "false");
//                     bufferPointer += 1;
//                     break;
//                 }
//                 case BasicType::CHAR: {
//                     char *value = (char*) bufferPointer;
//                     printf("Value(CHAR): %c \n", *value);
//                     bufferPointer += 1;
//                     break;
//                 }
//                 case BasicType::FLOAT: {
//                     float *value = (float*) bufferPointer;
//                     printf("Value(FLOAT): %f \n", *value);
//                     bufferPointer += 4;
//                     break;
//                 }
//                 case BasicType::DOUBLE: {
//                     double *value = (double*) bufferPointer;
//                     printf("Value(DOUBLE): %f \n", *value);
//                     bufferPointer += 8;
//                     break;
//                 }
//                 default:
//                     break;
//             }
//         }
//     }
// }

// void* getInputBuffer(NES::Runtime::BufferManager* buffMgr) {
//     const uint64_t numTuples = 2;
//     auto inputBuffer = buffMgr->getBufferBlocking();

//     // Create testTupleStruct. Load inputBuffer with testTupleStruct. Write sample array with testTupleStruct to inputBuffer.
//     struct __attribute__((packed)) testTupleStruct{
//         int64_t a;
//     };
//     testTupleStruct testTuplesArray[numTuples] = { testTupleStruct{1}, testTupleStruct{4} };
//     auto inputBufferPointer = inputBuffer.getBuffer<testTupleStruct>();
//     memcpy(inputBufferPointer, &testTuplesArray, sizeof(testTupleStruct) * numTuples);
//     inputBuffer.setNumberOfTuples(numTuples);

//     // Return inputBuffer pointer as void*.
//     return std::addressof(inputBuffer);
// }

// TEST(MLIRNESIRTest_Loop, simpleNESIRLoop) {
//     const uint64_t numTuples = 2;
//     auto buffMgr = new NES::Runtime::BufferManager(); //shared pointer causes crash (destructor problem)

//     // Create InputBuffer and fill it with data.
//     auto inputBuffer = buffMgr->getBufferBlocking();
//     struct __attribute__((packed)) testTupleStruct{
//         int8_t a;
//         int64_t b;
//     };
//     testTupleStruct testTuplesArray[numTuples] = { testTupleStruct{1, 2}, testTupleStruct{3,4} };
//     auto inputBufferPointer = inputBuffer.getBuffer<testTupleStruct>();
//     memcpy(inputBufferPointer, &testTuplesArray, sizeof(testTupleStruct) * numTuples);
//     inputBuffer.setNumberOfTuples(numTuples);

//     auto outputBuffer = buffMgr->getBufferBlocking();                

//     // Loop condition constants.
//     OperationPtr constIOp = std::make_shared<ConstantIntOperation>("i", 0, 64);
//     OperationPtr constOneOp = std::make_shared<ConstantIntOperation>("constOne", 1, 64);

//     // Loop BodyBlock Operations
//     auto inputAddressOp = std::make_shared<AddressOperation>("inTBAddressOp", NES::BasicType::INT64, 9, 1, "inputDataBuffer");
//     auto loadOp = std::make_shared<LoadOperation>("loadTBValOp", inputAddressOp, "inTBAddressOp");
//     auto constOp = std::make_shared<ConstantIntOperation>("constInt42Op", 42, 64);
//     auto addOp = std::make_shared<AddIntOperation>("inputAddOp", "loadTBValOp", "constInt42Op");
//     auto outputAddressOp = std::make_shared<AddressOperation>("outTBAddressOp", NES::BasicType::INT64, 8, 0, "outputDataBuffer");
//     auto storeOp = std::make_shared<StoreOperation>("inputAddOp", "outTBAddressOp");
//     auto loopIncAdd = std::make_shared<AddIntOperation>("loopIncAdd", "i", "constOne");

//     // Loop BodyBlock
//     std::vector<OperationPtr> loopOps{inputAddressOp, loadOp, constOp, addOp, outputAddressOp, storeOp, loopIncAdd};
//     std::vector<std::string> loopArguments{"inputDataBuffer", "outputDataBuffer", "numTuples", "i", "constOne"};
//     BasicBlockPtr loopBodyBlock = std::make_shared<BasicBlock>("loopBlock", loopOps, loopArguments, 1);
//     OperationPtr loopBodyTerminatorOp = std::make_shared<BranchOperation>(BranchOperation::LoopLastBranch, loopBodyBlock);
//     loopBodyBlock->addOperation(loopBodyTerminatorOp);

//     // Loop Header(IfOperation(LoopBodyBlock, ExecuteReturnBlock))
//     OperationPtr executeReturnOp = std::make_shared<ReturnOperation>(0);
//     std::vector<OperationPtr> executeReturnBlockOps{executeReturnOp};
//     std::vector<std::string> executeReturnBlockArgs{};
//     BasicBlockPtr executeReturnBlock = std::make_shared<BasicBlock>("loopBlock", executeReturnBlockOps, executeReturnBlockArgs, 1);
//     OperationPtr ifCompareOp = std::make_shared<CompareOperation>("loopCompare", "i", "numTuples", CompareOperation::ISLT);
//     OperationPtr loopIfOp = std::make_shared<IfOperation>("loopCompare", loopBodyBlock, executeReturnBlock);
//     std::vector<OperationPtr> loopHeaderBBOps{ifCompareOp, loopIfOp};
//     std::vector<std::string> loopHeaderArgs{"inputDataBuffer", "outputDataBuffer", "i", "constOne", "numTuples"};
//     BasicBlockPtr loopHeaderBlock = std::make_shared<BasicBlock>("loopHeaderBlock", loopHeaderBBOps, loopHeaderArgs, 1);

//     // Loop Operation -> loopBranchOp -> loopHeaderBlock -> loopIfOp -> (loopBodyBlock | executeEndBlock)
//     NES::OperationPtr loopOperation = std::make_shared<LoopOperation>(LoopOperation::ForLoop, loopHeaderBlock);


//     // Execute Function
//     // Execute Head Operations
//     std::vector<std::string> getInputDataBufArgs{"inputTupleBuffer"};
//     std::vector<std::string> getOutputDataBufArgs{"outputTupleBuffer"};
//     OperationPtr inputDataBufferProxy = std::make_shared<ProxyCallOperation>(Operation::GetDataBuffer, "inputDataBuffer", getInputDataBufArgs);
//     OperationPtr outputtDataBufferProxy = std::make_shared<ProxyCallOperation>(Operation::GetDataBuffer, "outputDataBuffer", getOutputDataBufArgs);
//     OperationPtr numTuplesProxy = std::make_shared<ProxyCallOperation>(Operation::GetNumTuples, "numTuples", getInputDataBufArgs);

//     std::vector<OperationPtr> executeBlockOps{inputDataBufferProxy, outputtDataBufferProxy, numTuplesProxy, constIOp, constOneOp, loopOperation};
//     std::vector<std::string> executeBodyBlockArgs{"inputTupleBuffer", "outputTupleBuffer"};
//     NES::BasicBlockPtr executeBodyBlock = std::make_shared<NES::BasicBlock>("executeFuncBB", executeBlockOps, executeBodyBlockArgs, 0);
//     std::vector<BasicType> executeArgTypes{ Operation::INT8PTR, Operation::INT8PTR};
//     std::vector<std::string> executeArgNames{ "inputTupleBuffer", "outputTupleBuffer"};
//     auto executeFuncOp = std::make_shared<FunctionOperation>("execute", executeBodyBlock, executeArgTypes, executeArgNames, Operation::INT64);

//     // NESIR
//     NES::NESIR nesIR(executeFuncOp);

//     // NESIR to MLIR
//     auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
//     int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(&nesIR);
//     assert(loadedModuleSuccess == 0);

//     // Register buffers and functions with JIT and run module.
//     const std::vector<std::string> symbolNames{"printValueFromMLIR"};
//     // Order must match symbolNames order!
//     const std::vector<llvm::JITTargetAddress> jitAddresses{
//             llvm::pointerToJITTargetAddress(&printValueFromMLIR),
//     };
//     mlirUtility->runJit(symbolNames, jitAddresses, false, std::addressof(inputBuffer), std::addressof(outputBuffer));

//     // Print OutputBuffer after execution.
//     std::vector<BasicType> types{Operation::INT64};
//     auto outputBufferPointer = outputBuffer.getBuffer<int8_t>();
//     printBuffer(types, numTuples, outputBufferPointer);
// }


// }// namespace NES