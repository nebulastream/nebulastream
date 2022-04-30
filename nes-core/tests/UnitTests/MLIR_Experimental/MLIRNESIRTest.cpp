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

void* getInputBuffer(NES::Runtime::BufferManager* buffMgr) {
    const uint64_t numTuples = 3;
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
    const uint64_t numTuples = 3;
    auto buffMgr = new NES::Runtime::BufferManager(); //shared pointer causes crash (destructor problem)
    void* inputBufferPtr = getInputBuffer(buffMgr);
    auto outPutBuffer = buffMgr->getBufferBlocking();
    void* outputBufferPtr = std::addressof(outPutBuffer);
    // Add Operation
    auto loadOp = std::make_shared<LoadOperation>(0);
    auto constOp = std::make_shared<ConstantIntOperation>(9);
    auto addOp = std::make_shared<AddIntOperation>(std::move(loadOp), std::move(constOp));
    // Store Operation
    std::vector<OperationPtr> storeOps{addOp};
    std::vector<uint64_t> storeIdxs{0};
    auto storeOp = std::make_shared<StoreOperation>(outputBufferPtr, storeOps, storeIdxs);
    // Loop BasicBlock
    std::vector<OperationPtr> loopOps{storeOp};
    auto loopBlock = new LoopBasicBlock(loopOps, nullptr, inputBufferPtr, numTuples);
    printf("Upper Limit: %ld\n", loopBlock->getUpperLimit());
    assert(true);
}


}// namespace NES::Compiler
#endif //MLIR_COMPILER