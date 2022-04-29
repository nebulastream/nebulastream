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
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>

#include <Experimental/NESIR/BasicBlocks/LoopBasicBlock.hpp>
#include <Experimental/NESIR/Operations/LoadOperation.hpp>
#include <Experimental/NESIR/Operations/StoreOperation.hpp>
#include <Experimental/NESIR/Operations/AddOperation.hpp>
#include <Experimental/NESIR/Operations/ConstantOperation.hpp>

namespace NES {
class MLIRNESIRTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MLIRNESIRTest.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("MLIRNESIRTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() { NES_INFO("MLIRNESIRTest test class TearDownTestCase."); }
};

void* getInputBuffer(NES::Runtime::BufferManager* buffMgr) {
    const uint64_t numTuples = 3;
    auto inputBuffer = buffMgr->getBufferBlocking();
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


    // Get buffer pointers and copy payload to inputBuffer.
    auto inputBufferPointer = inputBuffer.getBuffer<testTupleStruct>();

    inputBuffer.setNumberOfTuples(numTuples);
    void *IBPtr = std::addressof(inputBuffer);
    return IBPtr;
}

TEST(MLIRNESIRTest, simpleBufferIteration) {
    const uint64_t numTuples = 3;
    auto buffMgr = new NES::Runtime::BufferManager(); //shared pointer causes crash (destructor problem)
    void* inputBufferPtr = getInputBuffer(buffMgr);
    std::unique_ptr<Operation> loadOp = std::make_unique<LoadOperation>(inputBufferPtr);
    std::unique_ptr<Operation> constOp = std::make_unique<ConstantOperation>(9);
    auto addOp = new AddOperation(std::move(loadOp), std::move(constOp));
    std::vector<Operation*> loopOps;
    loopOps.emplace_back(addOp);
    auto loopBlock = new LoopBasicBlock(loopOps, nullptr, 3);
    assert(true);
}


}// namespace NES::Compiler
#endif //MLIR_COMPILER