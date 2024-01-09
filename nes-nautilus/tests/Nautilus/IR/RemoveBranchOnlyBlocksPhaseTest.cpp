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

#include <BaseIntegrationTest.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Runtime/BufferManager.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <unordered_map>

namespace NES::Nautilus {

class RemoveBranchOnlyBlocksPhaseTest : public Testing::BaseUnitTest, public AbstractCompilationBackendTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TraceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TraceTest test class.");
    }

    // Takes a Nautilus function, creates the trace, converts it Nautilus IR, and applies all available phases.
    std::shared_ptr<NES::Nautilus::IR::IRGraph> createTraceAndApplyPhases(std::function<Value<>()> nautilusFunction) {
        auto execution = Nautilus::Tracing::traceFunctionWithReturn([nautilusFunction]() {
            return nautilusFunction();
        });
        auto executionTrace = ssaCreationPhase.apply(std::move(execution));
        auto ir = irCreationPhase.apply(executionTrace);
        removeBrOnlyBlocksPhase.apply(ir);
        return ir;
    }

    /**
     * @brief A helper struct that represents the correct state of a BasicBlock in the IR.
     */
    struct CorrectBlockValues {
        std::unordered_set<std::string> correctPredecessors;
        std::unordered_set<std::string> correctNextBlocks;
    };
    using CorrectBlockValuesPtr = std::unique_ptr<CorrectBlockValues>;

    /**
     * @brief Create a BasicBlock that represents the correct state of a BasicBlock from a given IR.
     * 
     * @param correctBlocks: A HashMap that the newly created correct block is inserted into.
     * @param correctBlockId: The ID of the block that we want to check state correctness for.
     * @param correctPredecessors: A set of predecessor Ids that the checked block must contain.
     * @param correctNextBlocks: A set of IDs of basic blocks that the checked block must point to.
     */
    void createCorrectBlock(std::unordered_map<std::string, CorrectBlockValuesPtr>& correctBlocks,
                            std::string correctBlockId,
                            std::unordered_set<std::string> correctPredecessors,
                            std::unordered_set<std::string> correctNextBlocks) {
        correctBlocks.emplace(
            std::pair{correctBlockId,
                      std::make_unique<CorrectBlockValues>(CorrectBlockValues{correctPredecessors, correctNextBlocks})});
    }

    /**
     * @brief Iterate over IR using depth-first-search. Check for every block whether it is in a correct state.
     * 
     * @param currentBlock: BasicBlock that is currently checked. Is body-block of root operation at first.
     * @param correctBlocks: A HashMap that contains the correct state for all blocks in the IR.
     * @return true, if all blocks were in correct state, and false, if at least one block was in incorrect state.
     */
    bool checkIRForCorrectness(IR::BasicBlockPtr currentBlock,
                               const std::unordered_map<std::string, CorrectBlockValuesPtr>& correctBlocks) {
        std::stack<IR::BasicBlockPtr> blocksToVisit;
        std::unordered_set<std::string> visitedBlocks;
        blocksToVisit.push(currentBlock);
        bool allRequiredBlocksAreVisited;
        bool predecessorsAreCorrect = true;
        bool nextBlocksAreCorrect = true;
        do {
            visitedBlocks.emplace(currentBlock->getIdentifier());
            currentBlock = blocksToVisit.top();
            // The remove branch only phase should have removed all branch only blocks leaving only 'correct' blocks.
            if (correctBlocks.contains(currentBlock->getIdentifier())) {
                // If the currentBlock has predecessors, check that the number of predecessors matches the correct
                // number of predecessors and check that all existing predecessors are correct.
                if (correctBlocks.at(currentBlock->getIdentifier())->correctPredecessors.size()
                    == currentBlock->getPredecessors().size()) {
                    for (auto predecessorBlock : currentBlock->getPredecessors()) {
                        if (!correctBlocks.at(currentBlock->getIdentifier())
                                 ->correctPredecessors.contains(predecessorBlock.lock()->getIdentifier())) {
                            predecessorsAreCorrect = false;
                            break;
                        }
                    }
                } else {
                    predecessorsAreCorrect = false;
                }
                // Every Block MUST have a terminator operation. Check whether the terminator operation points to the correct next blocks.
                if (currentBlock->getTerminatorOp()->getOperationType() == IR::Operations::Operation::OperationType::BranchOp) {
                    auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(currentBlock->getTerminatorOp());
                    nextBlocksAreCorrect =
                        correctBlocks.at(currentBlock->getIdentifier())
                            ->correctNextBlocks.contains(branchOp->getNextBlockInvocation().getBlock()->getIdentifier());
                } else if (currentBlock->getTerminatorOp()->getOperationType()
                           == IR::Operations::Operation::OperationType::IfOp) {
                    auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(currentBlock->getTerminatorOp());
                    nextBlocksAreCorrect =
                        correctBlocks.at(currentBlock->getIdentifier())
                            ->correctNextBlocks.contains(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier())
                        && correctBlocks.at(currentBlock->getIdentifier())
                               ->correctNextBlocks.contains(ifOp->getFalseBlockInvocation().getBlock()->getIdentifier());
                } else if (currentBlock->getTerminatorOp()->getOperationType()
                           == IR::Operations::Operation::OperationType::ReturnOp) {
                    nextBlocksAreCorrect = correctBlocks.at(currentBlock->getIdentifier())->correctNextBlocks.empty();
                } else {
                    NES_NOT_IMPLEMENTED();
                }
            } else {
                allRequiredBlocksAreVisited = false;
            }
            blocksToVisit.pop();
            auto terminatorOp = currentBlock->getTerminatorOp();
            if (terminatorOp->getOperationType() == IR::Operations::Operation::OperationType::BranchOp) {
                auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
                if (!visitedBlocks.contains(branchOp->getNextBlockInvocation().getBlock()->getIdentifier())) {
                    blocksToVisit.emplace(branchOp->getNextBlockInvocation().getBlock());
                }
            } else if (terminatorOp->getOperationType() == IR::Operations::Operation::OperationType::IfOp) {
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
                if (!visitedBlocks.contains(ifOp->getFalseBlockInvocation().getBlock()->getIdentifier())) {
                    blocksToVisit.emplace(ifOp->getFalseBlockInvocation().getBlock());
                }
                if (!visitedBlocks.contains(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier())) {
                    blocksToVisit.emplace(ifOp->getTrueBlockInvocation().getBlock());
                }
            }
        } while (predecessorsAreCorrect && nextBlocksAreCorrect && !blocksToVisit.empty());
        return predecessorsAreCorrect;
    }
};

//==----------------------------------------------------------==//
//==------------------ NAUTILUS Phase TESTS -------------------==//
//==----------------------------------------------------------==//
Value<> simpleIfOperationWithoutFalseBranch_0() {
    Value agg = Value(0);
    if (agg < 40) {
        agg = agg + 10;
    } else {
    }
    return agg;
}
TEST_P(RemoveBranchOnlyBlocksPhaseTest, 0_SimpleIfOperationWithoutFalseBranch) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    // createCorrectBlock(correctBlocks, blockId, predecessors, nextBlocks)
    createCorrectBlock(correctBlocks, "0", {}, {"1", "3"});
    createCorrectBlock(correctBlocks, "1", {"0"}, {"3"});
    createCorrectBlock(correctBlocks, "3", {"0", "1"}, {});
    auto ir = createTraceAndApplyPhases(&simpleIfOperationWithoutFalseBranch_0);
    EXPECT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

Value<> doubleVerticalDiamondInTrueBranch() {
    Value agg = Value(0);
    if (agg < 50) {
        if (agg < 40) {
            agg = agg + 10;
        } else {
            agg = agg + 100;
        }
        if (agg > 60) {
            agg = agg + 1000;
        } else {
            agg = agg + 10000;
        }
    } else {
        agg = agg + 100000;
    }
    agg = agg + 1;
    return agg;
}
TEST_P(RemoveBranchOnlyBlocksPhaseTest, 2_doubleVerticalDiamondInTrueBranch) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    // createCorrectBlock(correctBlocks, blockId, predecessors, nextBlocks)
    createCorrectBlock(correctBlocks, "0", {}, {"1", "2"});
    createCorrectBlock(correctBlocks, "1", {"0"}, {"3", "4"});
    createCorrectBlock(correctBlocks, "2", {"0"}, {"9"});
    createCorrectBlock(correctBlocks, "3", {"1"}, {"8"});
    createCorrectBlock(correctBlocks, "4", {"1"}, {"8"});
    createCorrectBlock(correctBlocks, "8", {"3", "4"}, {"5", "6"});
    createCorrectBlock(correctBlocks, "5", {"8"}, {"9"});
    createCorrectBlock(correctBlocks, "6", {"8"}, {"9"});
    createCorrectBlock(correctBlocks, "9", {"5", "6", "2"}, {});
    auto ir = createTraceAndApplyPhases(&doubleVerticalDiamondInTrueBranch);
    EXPECT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

Value<> loopMergeBlockBeforeCorrespondingIfOperation_3() {
    Value agg = Value(0);
    Value limit = Value(1000);
    while (agg < limit) {
        if (agg < 350) {
            agg = agg + 1;
        } else {
            agg = agg + 3;
        }
    }
    return agg;
}
TEST_P(RemoveBranchOnlyBlocksPhaseTest, 3_loopMergeBlockBeforeCorrespondingIfOperation) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    // createCorrectBlock(correctBlocks, blockId, predecessors, nextBlocks)
    createCorrectBlock(correctBlocks, "0", {}, {"6"});
    createCorrectBlock(correctBlocks, "6", {"0", "3", "4"}, {"1", "2"});
    createCorrectBlock(correctBlocks, "1", {"6"}, {"3", "4"});
    createCorrectBlock(correctBlocks, "3", {"1"}, {"6"});
    createCorrectBlock(correctBlocks, "4", {"1"}, {"6"});
    createCorrectBlock(correctBlocks, "2", {"6"}, {});
    auto ir = createTraceAndApplyPhases(&loopMergeBlockBeforeCorrespondingIfOperation_3);
    EXPECT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

Value<> mergeLoopMergeBlockWithLoopFollowUp_4() {
    Value agg = Value(0);
    Value limit = Value(1000000);
    if (agg < 350) {
        agg = agg + 1;
    } else {
        agg = agg + 3;
    }
    while (agg < limit) {
        if (agg < 350) {
            agg = agg + 1;
        } else {
            agg = agg + 3;
        }
    }
    while (agg < limit) {
        agg = agg + 4;
    }
    return agg;
}
TEST_P(RemoveBranchOnlyBlocksPhaseTest, 4_mergeLoopMergeBlockWithLoopFollowUp) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    // createCorrectBlock(correctBlocks, blockId, predecessors, nextBlocks)
    createCorrectBlock(correctBlocks, "0", {}, {"1", "2"});
    createCorrectBlock(correctBlocks, "1", {"0"}, {"12"});
    createCorrectBlock(correctBlocks, "2", {"0"}, {"12"});
    createCorrectBlock(correctBlocks, "12", {"1", "2", "5", "6"}, {"3", "10"});
    createCorrectBlock(correctBlocks, "3", {"12"}, {"5", "6"});
    createCorrectBlock(correctBlocks, "5", {"3"}, {"12"});
    createCorrectBlock(correctBlocks, "6", {"3"}, {"12"});
    createCorrectBlock(correctBlocks, "10", {"8", "12"}, {"8", "9"});
    createCorrectBlock(correctBlocks, "8", {"10"}, {"10"});
    createCorrectBlock(correctBlocks, "9", {"10"}, {});
    auto ir = createTraceAndApplyPhases(&mergeLoopMergeBlockWithLoopFollowUp_4);
    EXPECT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

// Tests all registered compilation backends.
// To select a specific compilation backend use ::testing::Values("MLIR") instead of ValuesIn.
INSTANTIATE_TEST_CASE_P(testLoopCompilation,
                        RemoveBranchOnlyBlocksPhaseTest,
                        ::testing::ValuesIn(Backends::CompilationBackendRegistry::getPluginNames().begin(),
                                            Backends::CompilationBackendRegistry::getPluginNames().end()),
                        [](const testing::TestParamInfo<RemoveBranchOnlyBlocksPhaseTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus