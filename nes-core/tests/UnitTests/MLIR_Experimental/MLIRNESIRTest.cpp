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

void printBuffer(std::vector<Operation::BasicType> types,
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
    return std::addressof(inputBuffer);
}

TEST(MLIRNESIRTest, simpleNESIRCreation) {
    const uint64_t numTuples = 2;
    auto buffMgr = new NES::Runtime::BufferManager(); //shared pointer causes crash (destructor problem)

    // Create InputBuffer and fill it with data.
    auto inputBuffer = buffMgr->getBufferBlocking();
    struct __attribute__((packed)) testTupleStruct{
        int8_t a;
        int64_t b;
    };
    testTupleStruct testTuplesArray[numTuples] = { testTupleStruct{1, 2}, testTupleStruct{3,4} };
    auto inputBufferPointer = inputBuffer.getBuffer<testTupleStruct>();
    memcpy(inputBufferPointer, &testTuplesArray, sizeof(testTupleStruct) * numTuples);
    inputBuffer.setNumberOfTuples(numTuples);

    auto outputBuffer = buffMgr->getBufferBlocking();

    // Add Operation
    auto addressOp = std::make_shared<AddressOperation>(NES::Operation::BasicType::INT64, 9, 1, true);
    auto loadOp = std::make_shared<LoadOperation>(addressOp);
    auto constOp = std::make_shared<ConstantIntOperation>(9, 64);
    auto addOp = std::make_shared<AddIntOperation>(std::move(loadOp), std::move(constOp));

    // Store Operation
    auto storeAddressOp = std::make_shared<AddressOperation>(NES::Operation::BasicType::INT64, 8, 0, false);
    auto storeOp = std::make_shared<StoreOperation>(addOp, storeAddressOp);

    // Loop BasicBlock
    std::vector<OperationPtr> loopOps{storeOp};
    BasicBlockPtr loopBlock = std::make_unique<BasicBlock>(loopOps);
    NES::OperationPtr loopOperation = std::make_shared<LoopOperation>(std::move(loopBlock));

    // Execute Function
    std::vector<OperationPtr> executeBlockOps{loopOperation};
    NES::BasicBlockPtr executeBodyBlock = std::make_unique<NES::BasicBlock>(executeBlockOps);
    std::vector<Operation::BasicType> executeArgTypes{ Operation::INT8PTR, Operation::INT8PTR};
    std::vector<std::string> executeArgNames{ "InputBuffer", "OutputBuffer"};
    auto executeFuncOp = std::make_shared<FunctionOperation>("execute", std::move(executeBodyBlock),
                                                             executeArgTypes, executeArgNames, Operation::INT64);

    // NESIR
    std::vector<ExternalDataSourcePtr> externalDataSources{};
    NES::NESIR nesIR(executeFuncOp, externalDataSources);
    std::cout << externalDataSources.size() << '\n';

    // NESIR to MLIR
    auto mlirUtility = new MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(&nesIR);
    assert(loadedModuleSuccess == 0);

    // Register buffers and functions with JIT and run module.
    const std::vector<std::string> symbolNames{"printValueFromMLIR"};
    // Order must match symbolNames order!
    const std::vector<llvm::JITTargetAddress> jitAddresses{
            llvm::pointerToJITTargetAddress(&printValueFromMLIR),
    };
    mlirUtility->runJit(symbolNames, jitAddresses, false, std::addressof(inputBuffer), std::addressof(outputBuffer));

    // Print OutputBuffer after execution.
    std::vector<Operation::BasicType> types{Operation::INT64};
    auto outputBufferPointer = outputBuffer.getBuffer<int8_t>();
    printBuffer(types, numTuples, outputBufferPointer);
}


}// namespace NES