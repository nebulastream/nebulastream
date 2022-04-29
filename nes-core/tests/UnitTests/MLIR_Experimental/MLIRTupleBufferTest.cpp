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

#ifdef MLIR_COMPILER
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

#include "mlir/IR/AsmState.h"
#include "mlir/Pass/PassManager.h"
#include "mlir/Transforms/DialectConversion.h"
#include "llvm/ExecutionEngine/JITSymbol.h"

#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionNode.hpp>
#include <Experimental/utility/NESAbstractionUtility.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES {
class MLIRTupleBufferIteration : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MLIRBufferTest.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("MLIRBufferTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() { NES_INFO("MLIRBufferTest test class TearDownTestCase."); }
};

__attribute__((always_inline)) void printValueFromMLIR(uint64_t value) {
    printf("Result from getNumTuples: %ld\n\n", value);
}

// Test function that is called from within the MLIR
// Interesting 'intrinsics':
// extern "C" & inline -> alwaysInlinePass?
int printFromMLIR(NESAbstractionNode::BasicType type, int8_t *bufferPointer) {
    if(type == 0 || type == 1) {
        int8_t *value = (int8_t*) bufferPointer;
        printf("Value(INT8): %d \n", *value);
    } else if(type == 2) {
        int16_t *value = (int16_t*) bufferPointer;
        printf("Value(INT64): %d \n", *value);
    } else if(type == 3) {
        int32_t *value = (int32_t*) bufferPointer;
        printf("Value(INT64): %d \n", *value);
    } else if(type == 4) {
        int64_t *value = (int64_t*) bufferPointer;
        printf("Value(INT64): %ld \n", *value);
    } else if(type == 5) {
        float *value = (float*) bufferPointer;
        printf("Value(FLOAT): %f \n", *value);
    } else if(type == 6) {
        double *value = (double*) bufferPointer;
        printf("Value(DOUBLE): %f \n", *value);
    } else if(type == 7) {
        bool *value = (bool*) bufferPointer;
        printf("Value(BOOL): %s \n", (*value) ? "true" : "false");
    } else if(type == 8) {
        char *value = (char*) bufferPointer;
        printf("Value(CHAR): %c \n", *value);
    } else {
        int *value = (int*) bufferPointer;
        printf("Value: %d \n", *value);
    };
    return 0;
}

void printBuffer(std::vector<NESAbstractionNode::BasicType> types,
                 uint64_t numTuples, int8_t *bufferPointer) {
    for(uint64_t i = 0; i < numTuples; ++i) {
        printf("------------\nTuple Nr. %lu\n------------\n", i+1);
        for(auto type : types) {
            switch(type) {
                case NESAbstractionNode::BasicType::INT1: {
                    printf("Value(INT32): %d \n", *bufferPointer);
                    bufferPointer += 1;
                    break;
                }
                case NESAbstractionNode::BasicType::INT8: {
                    printf("Value(INT32): %d \n", *bufferPointer);
                    bufferPointer += 1;
                    break;
                }
                case NESAbstractionNode::BasicType::INT16: {
                    int16_t *value = (int16_t*) bufferPointer;
                    printf("Value(INT32): %d \n", *value);
                    bufferPointer += 2;
                    break;
                }
                case NESAbstractionNode::BasicType::INT32: {
                    int32_t *value = (int32_t*) bufferPointer;
                    printf("Value(INT32): %d \n", *value);
                    bufferPointer += 4;
                    break;
                }
                case NESAbstractionNode::BasicType::INT64: {
                    int64_t *value = (int64_t*) bufferPointer;
                    printf("Value(INT32): %ld \n", *value);
                    bufferPointer += 8;
                    break;
                }
                case NESAbstractionNode::BasicType::BOOLEAN: {
                    bool *value = (bool*) bufferPointer;
                    printf("Value(BOOL): %s \n", (*value) ? "true" : "false");
                    bufferPointer += 1;
                    break;
                }
                case NESAbstractionNode::BasicType::CHAR: {
                    char *value = (char*) bufferPointer;
                    printf("Value(CHAR): %c \n", *value);
                    bufferPointer += 1;
                    break;
                }
                case NESAbstractionNode::BasicType::FLOAT: {
                    float *value = (float*) bufferPointer;
                    printf("Value(FLOAT): %f \n", *value);
                    bufferPointer += 4;
                    break;
                }
                case NESAbstractionNode::BasicType::DOUBLE: {
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



TEST(MLIRTupleBufferIteration, simpleBufferIteration) {
    // Register any command line options.
    mlir::registerAsmPrinterCLOptions();
    mlir::registerMLIRContextCLOptions();
    mlir::registerPassManagerCLOptions();

    // Configuration
    const bool debugFromFile = false;
    const std::string mlirFilepath = "/home/rudi/mlir/generatedMLIR/locationTest.mlir";
    MLIRUtility::DebugFlags debugFlags{};
    debugFlags.comments = true;
    debugFlags.enableDebugInfo = false;
    debugFlags.prettyDebug = false;

    // Create NES TupleBuffers
    auto buffMgr = new NES::Runtime::BufferManager(); //shared pointer causes crash (destructor problem)

    auto inputBuffer = buffMgr->getBufferBlocking();
    auto outputBuffer = buffMgr->getBufferBlocking();

    // create input data
    const uint64_t numTuples = 3;
    std::vector<NESAbstractionNode::BasicType> types{
        NESAbstractionNode::BasicType::INT8,
        NESAbstractionNode::BasicType::INT32,
        NESAbstractionNode::BasicType::BOOLEAN,
        NESAbstractionNode::BasicType::FLOAT,
        NESAbstractionNode::BasicType::CHAR,
        NESAbstractionNode::BasicType::DOUBLE
    };
    std::vector<uint64_t> indices{0,1,2,3,4,5};

    struct __attribute__((packed)) testTupleStruct{
        int8_t a;
        int32_t b;
        bool c;
        float d;
        char e;
        double f;
    };
    testTupleStruct testTuplesArray[numTuples] =
        {
            testTupleStruct{1, 300, true, 2.3, 'a', 10.101010},
            testTupleStruct{4, 5, false, 1.12, 'b', 22.222222},
            testTupleStruct{6, 7, true, 1.7, 'c', 94.23}
        };

    // Create sample NESAbstractionTree create an MLIR Module for it.
    auto NESTree = NESAbstractionUtility::createSimpleNESAbstractionTree(numTuples, indices, types);
    auto mlirUtility = std::make_shared<MLIRUtility>(mlirFilepath, debugFromFile);
    if (int error = mlirUtility->loadAndProcessMLIR(NESTree, &debugFlags)) {
        assert(false);
    }

    // Get buffer pointers and copy payload to inputBuffer.
    auto inputBufferPointer = inputBuffer.getBuffer<testTupleStruct>();

    inputBuffer.setNumberOfTuples(numTuples);
    void *IBPtr = std::addressof(inputBuffer);

    auto outputBufferPointer = outputBuffer.getBuffer<int8_t>();
    memcpy(inputBufferPointer, &testTuplesArray, sizeof(testTupleStruct) * numTuples);

    // Register buffers and functions with JIT and run module.
    const std::vector<std::string> symbolNames{"printFromMLIR",
                                               "printValueFromMLIR",
                                               "IB", "OB",
                                               "IBPtr"};
    // Order must match symbolNames order!
    const std::vector<llvm::JITTargetAddress> jitAddresses{
        llvm::pointerToJITTargetAddress(&printFromMLIR),
        llvm::pointerToJITTargetAddress(&printValueFromMLIR),
        llvm::pointerToJITTargetAddress(inputBufferPointer),
        llvm::pointerToJITTargetAddress(outputBufferPointer),
        llvm::pointerToJITTargetAddress(IBPtr)};
    mlirUtility->runJit(symbolNames, jitAddresses, true);

    printBuffer(types, numTuples, outputBufferPointer);
    assert(true);
}


}// namespace NES::Compiler
#endif //MLIR_COMPILER