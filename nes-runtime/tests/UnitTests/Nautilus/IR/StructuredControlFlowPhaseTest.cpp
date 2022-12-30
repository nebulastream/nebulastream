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

#include "Nautilus/IR/BasicBlocks/BasicBlock.hpp"
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <unordered_map>

namespace NES::Nautilus {
/**
 * @brief The StructuredControlFlowPhaseTest contains a battery of tests that all do the following:
 *          -> they call a unique(in the scope of this test) Nautilus function with the same name as the test
 *          -> they apply symbolic execution to the Nautilus function to get a function trace
 *          -> the function trace passes the SSA-, and the TraceToIRConversion phases, which generates an IR graph
 *          -> the IR graph passes the AddPredecessor- and Remove-Branch-Only phases, which operate on the IR graph
 *          -> then, the IR graph passes the FindLoopHeader- and the CreateIfOperation phases
 *          -> Lastly, we iterate over the resulting IR graph and check whether loop blocks have been marked as loop
 *             blocks, and whether if-operations have been matched with their correct corresponding merge-blocks
 */
class StructuredControlFlowPhaseTest : public Testing::NESBaseTest, public AbstractCompilationBackendTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StructuredControlFlowPhaseTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StructuredControlFlowPhaseTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down StructuredControlFlowPhaseTest test class."); }

    // Takes a Nautilus function, creates the trace, converts it Nautilus IR, and applies all available phases.
    std::vector<IR::BasicBlockPtr> createTraceAndApplyPhases(std::function<Value<>()> nautilusFunction) {
        auto execution = Nautilus::Tracing::traceFunctionWithReturn([nautilusFunction]() {
            return nautilusFunction();
        });
        auto executionTrace = ssaCreationPhase.apply(std::move(execution));
        auto ir = irCreationPhase.apply(executionTrace);
        auto dpsSortedGraphNodes = enumerateIRForTests(ir);
        removeBrOnlyBlocksPhase.apply(ir);
        structuredControlFlowPhase.apply(ir);
        return dpsSortedGraphNodes;
    }

    struct CorrectBlockValues {
        uint32_t correctNumberOfBackLinks;
        std::string correctMergeBlockId;
    };
    using CorrectBlockValuesPtr = std::unique_ptr<CorrectBlockValues>;
    void createCorrectBlock(std::unordered_map<std::string, CorrectBlockValuesPtr>& correctBlocks,
                            std::string correctBlockId,
                            uint32_t correctNumberOfBackLinks,
                            std::string correctMergeBlockId) {
        correctBlocks.emplace(
            std::pair{correctBlockId,
                      std::make_unique<CorrectBlockValues>(CorrectBlockValues{correctNumberOfBackLinks, correctMergeBlockId})});
    }

    /**
     * @brief Takes a graph IR, enumerates all blocks depth-first, and returns a vector containing all blocks.
     * 
     * @param ir: Graph ir that is traversed depth-first-search (dps).
     * @return std::vector<IR::BasicBlockPtr>: A vector containing all blocks of the ir-graph sorted by dps.
     */
    std::vector<IR::BasicBlockPtr> enumerateIRForTests(std::shared_ptr<IR::IRGraph> ir) {
        std::stack<IR::BasicBlockPtr> newBlocks;
        std::unordered_set<IR::BasicBlock*> visitedBlocks;
        std::vector<IR::BasicBlockPtr> dpsSortedIRGraph;

        uint32_t currentId = 0;
        newBlocks.emplace(ir->getRootOperation()->getFunctionBasicBlock());
        do {
            visitedBlocks.emplace(newBlocks.top().get());
            newBlocks.top()->setIdentifier(std::to_string(currentId));
            ++currentId;
            dpsSortedIRGraph.emplace_back(newBlocks.top());
            auto nextBlocks = newBlocks.top()->getNextBlocks();
            newBlocks.pop();
            if (nextBlocks.second && !visitedBlocks.contains(nextBlocks.first.get())) {
                newBlocks.emplace(nextBlocks.second);
            }
            if (nextBlocks.first && !visitedBlocks.contains(nextBlocks.first.get())) {
                newBlocks.emplace(nextBlocks.first);
            }
        } while (!newBlocks.empty());
        return dpsSortedIRGraph;
    }

    bool checkIRForCorrectness(const std::vector<IR::BasicBlockPtr>& dpsSortedBlocks,
                               const std::unordered_map<std::string, CorrectBlockValuesPtr>& correctBlocks) {
        bool mergeBlocksAreCorrect = true;
        bool backLinksAreCorrect = true;
        for (auto currentBlock : dpsSortedBlocks) {
            if (currentBlock->getTerminatorOp()->getOperationType() == IR::Operations::Operation::IfOp) {
                // Check that the currentBlock is actually part of the solution set.
                if (correctBlocks.contains(currentBlock->getIdentifier())) {
                    auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(currentBlock->getTerminatorOp());
                    // Check that the number of loop back edges is set correctly.
                    backLinksAreCorrect = currentBlock->getNumLoopBackEdges()
                        == correctBlocks.at(currentBlock->getIdentifier())->correctNumberOfBackLinks;
                    if (!backLinksAreCorrect) {
                        NES_ERROR("\nBlock -" << currentBlock->getIdentifier() << "- contained -"
                                              << currentBlock->getNumLoopBackEdges() << "- backLinks instead of: -"
                                              << correctBlocks.at(currentBlock->getIdentifier())->correctNumberOfBackLinks
                                              << "-.");
                    }
                    // Check that the merge-block id is set correctly, if the if-operation has a merge-block.
                    auto correctMergeBlockId = correctBlocks.at(currentBlock->getIdentifier())->correctMergeBlockId;
                    if (!correctMergeBlockId.empty()) {
                        if (!ifOp->getMergeBlock()) {
                            NES_ERROR("CurrentBlock: " << currentBlock->getIdentifier() << " did not contain a merge block"
                                                       << " even though the solution suggest it has a merge-block with id: "
                                                       << correctMergeBlockId);
                            mergeBlocksAreCorrect = false;
                        } else {
                            bool correctMergeBlock = ifOp->getMergeBlock()->getIdentifier() == correctMergeBlockId;
                            mergeBlocksAreCorrect &= correctMergeBlock;
                            if (!correctMergeBlock) {
                                NES_ERROR("\nMerge-Block mismatch for block "
                                          << currentBlock->getIdentifier() << ": " << ifOp->getMergeBlock()->getIdentifier()
                                          << " instead of "
                                          << correctBlocks.at(currentBlock->getIdentifier())->correctMergeBlockId
                                          << "(correct).");
                            }
                        }
                    } else {
                        bool noMergeBlockCorrectlySet = !ifOp->getMergeBlock();
                        mergeBlocksAreCorrect &= noMergeBlockCorrectlySet;
                        if (!noMergeBlockCorrectlySet) {
                            NES_ERROR("The current merge block: " << currentBlock->getIdentifier()
                                                                  << " contains a merge-block with id: "
                                                                  << ifOp->getMergeBlock()->getIdentifier()
                                                                  << ", even though it should not contain a merge-block.");
                        }
                    }
                } else {
                    mergeBlocksAreCorrect = false;
                    NES_ERROR("CurrentBlock with id: "
                              << currentBlock->getIdentifier()
                              << " was not part of solution set(correctBlocks), but it contains an if-operation.");
                }
            } else {
                if (correctBlocks.contains(currentBlock->getIdentifier())) {
                    mergeBlocksAreCorrect = false;
                    NES_ERROR("CurrentBlock with id: "
                              << currentBlock->getIdentifier()
                              << " was part of solution set(correctBlocks), but it does not contain an if-operation.");
                }
            }
        }
        return mergeBlocksAreCorrect && backLinksAreCorrect;
    }
};

//==----------------------------------------------------------==//
//==------------------ NAUTILUS PHASE TESTS ------------------==//
//==----------------------------------------------------------==//
Value<> threeIfOperationsOneNestedThreeMergeBlocks_1() {
    Value agg = Value(0);
    if (agg < 40) {
        agg = agg + 10;
    } else {
        agg = agg + 100;
    }
    if (agg < 50) {
        if (agg > 60) {
            agg = agg + 1000;
        } else {
            agg = agg + 10000;
        }
        agg = agg + 1;
    } else {
        agg = agg + 100000;
    }
    agg = agg + 1;
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 1_threeIfOperationsOneNestedThreeMergeBlocks) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "2");
    createCorrectBlock(correctBlocks, "2", 0, "6");
    createCorrectBlock(correctBlocks, "3", 0, "5");
    auto dpsSortedGraphNodes = createTraceAndApplyPhases(&threeIfOperationsOneNestedThreeMergeBlocks_1);
    ASSERT_EQ(checkIRForCorrectness(dpsSortedGraphNodes, correctBlocks), true);
}

Value<> doubleVerticalDiamondInTrueBranch_2() {
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
TEST_P(StructuredControlFlowPhaseTest, 2_doubleVerticalDiamondInTrueBranch) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "6");
    createCorrectBlock(correctBlocks, "1", 0, "3");
    createCorrectBlock(correctBlocks, "3", 0, "6");
    auto dpsSortedBlocks = createTraceAndApplyPhases(&doubleVerticalDiamondInTrueBranch_2);
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> doubleHorizontalDiamondWithOneMergeBlockThatAlsoIsIfBlock_3() {
    Value agg = Value(0);
    if (agg < 50) {
        if (agg < 40) {
            agg = agg + 10;
        } else {
            agg = agg + 100;
        }
    } else {
        if (agg > 60) {
            agg = agg + 1000;
        } else {
            agg = agg + 10000;
        }
    }
    if (agg > 60) {
        agg = agg + 1000;
    } else {
        agg = agg + 10000;
    }
    return agg;
}
// Breaks in release mode.
TEST_P(StructuredControlFlowPhaseTest, 3_doubleHorizontalDiamondWithOneMergeBlockThatAlsoIsIfBlock) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "5");
    createCorrectBlock(correctBlocks, "1", 0, "5");
    createCorrectBlock(correctBlocks, "5", 0, "7");
    createCorrectBlock(correctBlocks, "10", 0, "5");
    auto dpsSortedBlocks = createTraceAndApplyPhases(&doubleHorizontalDiamondWithOneMergeBlockThatAlsoIsIfBlock_3);
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> oneMergeBlockThatClosesOneIfAndBecomesMergeForTwo_4() {
    Value agg = Value(0);
    if (agg < 50) {
        if (agg > 60) {
            agg = agg + 1000;
        } else {
            if (agg < 40) {
                agg = agg + 10;
            } else {
                agg = agg + 100;
            }
        }
    } else {
        agg = agg + 100000;
    }
    agg = agg + 1;
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 4_oneMergeBlockThatClosesOneIfAndBecomesMergeForTwo) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "5");
    createCorrectBlock(correctBlocks, "1", 0, "5");
    createCorrectBlock(correctBlocks, "6", 0, "5");
    auto dpsSortedBlocks = createTraceAndApplyPhases(&oneMergeBlockThatClosesOneIfAndBecomesMergeForTwo_4);
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsFollowedUpByLoopHeader_5() {
    Value agg = Value(0);
    Value limit = Value(0);
    if (agg < 50) {
        if (agg > 60) {
            agg = agg + 1000;
        } else {
            if (agg < 40) {
                agg = agg + 10;
            } else {
                agg = agg + 100;
            }
        }
    } else {
        agg = agg + 100000;
    }
    for (Value j = 0; j < agg; j = j + 1) {
        agg = agg + 1;
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 5_oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsFollowedUpByLoopHeader) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "5");
    createCorrectBlock(correctBlocks, "1", 0, "5");
    createCorrectBlock(correctBlocks, "6", 1, "");
    createCorrectBlock(correctBlocks, "9", 0, "5");
    auto dpsSortedBlocks =
        createTraceAndApplyPhases(&oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsFollowedUpByLoopHeader_5);
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsLoopHeader_6() {
    Value agg = Value(0);
    Value limit = Value(1000000);
    if (agg < 50) {
        if (agg > 60) {
            agg = agg + 1000;
        } else {
            if (agg < 40) {
                agg = agg + 10;
            } else {
                agg = agg + 100;
            }
        }
    } else {
        agg = agg + 100000;
    }
    while (agg < limit) {
        agg = agg + 1;
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 6_oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsLoopHeader) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "6");
    createCorrectBlock(correctBlocks, "1", 0, "6");
    createCorrectBlock(correctBlocks, "6", 1, "");
    createCorrectBlock(correctBlocks, "9", 0, "6");
    auto dpsSortedBlocks = createTraceAndApplyPhases(&oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsLoopHeader_6);
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> loopMergeBlockBeforeCorrespondingIfOperation_7() {
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
TEST_P(StructuredControlFlowPhaseTest, 7_loopMergeBlockBeforeCorrespondingIfOperation) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "2", 2, "");
    createCorrectBlock(correctBlocks, "3", 0, "2");
    auto dpsSortedBlocks = createTraceAndApplyPhases(&loopMergeBlockBeforeCorrespondingIfOperation_7);
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> mergeLoopMergeBlockWithLoopFollowUp_8() {
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
TEST_P(StructuredControlFlowPhaseTest, 8_mergeLoopMergeBlockWithLoopFollowUp) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "4");
    createCorrectBlock(correctBlocks, "4", 2, "");
    createCorrectBlock(correctBlocks, "5", 0, "4");
    createCorrectBlock(correctBlocks, "9", 1, "");
    auto dpsSortedBlocks = createTraceAndApplyPhases(&mergeLoopMergeBlockWithLoopFollowUp_8);
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> LoopHeaderWithNineBackLinks_9() {
    Value agg = Value(0);
    Value limit = Value(1000);
    while (agg < limit) {
        if (agg < 350) {
            if (agg < 350) {
                if (agg < 350) {
                    agg = agg + 1;
                } else {
                    agg = agg + 3;
                }
                if (agg < 350) {

                } else {
                    if (agg < 350) {
                        agg = agg + 1;
                    } else {
                        agg = agg + 3;
                    }
                    agg = agg + 4;
                }
            } else {
            }
        } else {
            if (agg < 350) {
                if (agg < 350) {
                    if (agg < 350) {
                        agg = agg + 1;
                    } else {
                        agg = agg + 1;
                    }
                } else {
                }
            } else {
                if (agg < 350) {

                } else {
                    if (agg < 350) {
                        agg = agg + 1;
                    } else {
                    }
                }
            }
        }
    }
    while (agg < limit) {
        agg = agg + 4;
    }
    return agg;
}
// Breaks in release mode.
TEST_P(StructuredControlFlowPhaseTest, 9_loopHeaderWithNineBackLinks) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "9", 9, "");
    createCorrectBlock(correctBlocks, "10", 0, "9");
    createCorrectBlock(correctBlocks, "11", 0, "9");
    createCorrectBlock(correctBlocks, "12", 0, "14");
    createCorrectBlock(correctBlocks, "14", 0, "9");
    createCorrectBlock(correctBlocks, "16", 0, "18");
    createCorrectBlock(correctBlocks, "22", 0, "9");
    createCorrectBlock(correctBlocks, "23", 0, "9");
    createCorrectBlock(correctBlocks, "24", 0, "9");
    createCorrectBlock(correctBlocks, "28", 0, "9");
    createCorrectBlock(correctBlocks, "30", 0, "9");
    createCorrectBlock(correctBlocks, "34", 1, "");
    auto dpsSortedBlocks = createTraceAndApplyPhases(&LoopHeaderWithNineBackLinks_9);
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> mergeLoopBlock_10() {
    Value agg = Value(0);
    Value limit = Value(10);
    if (agg < 350) {
        agg = agg + 1;
    } else {
        agg = agg + 3;
    }
    while (agg < limit) {
        agg = agg + 4;
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 10_mergeLoopBlock) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "3");
    createCorrectBlock(correctBlocks, "3", 1, "");
    auto dpsSortedBlocks = createTraceAndApplyPhases(&mergeLoopBlock_10);
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> IfOperationFollowedByLoopWithDeeplyNestedIfOperationsWithSeveralNestedLoops_11() {
    Value agg = Value(0);
    Value limit = Value(1000000);
    if (agg < 150) {
        agg = agg + 1;
    } else {
        if (agg < 150) {
            agg = agg + 1;
        } else {
            agg = agg + 1;
        }
    }
    for (Value start = 0; start < 10; start = start + 1) {
        if (agg < 50) {          //3
            while (agg < limit) {//9
                agg = agg + 1;
            }
        } else {
            for (Value start = 0; start < 10; start = start + 1) {
                agg = agg + 1;
            }
        }
        if (agg < 150) {//16

        } else {
            if (agg < 250) {
                while (agg < limit) {
                    if (agg < 350) {
                        agg = agg + 1;
                    }
                }
                for (Value start = 0; start < 10; start = start + 1) {
                    agg = agg + 1;
                }
            }
            if (agg < 450) {
                agg = agg + 1;
            } else {
                agg = agg + 2;
            }
            if (agg < 550) {
                agg = agg + 1;
            } else {
                while (agg < limit) {
                    if (agg < 350) {
                        agg = agg + 1;
                    }
                }
            }
        }
        // 41
    }
    return agg;
}
// Breaks in release mode.
TEST_P(StructuredControlFlowPhaseTest, 11_IfOperationFollowedByLoopWithDeeplyNestedIfOperationsWithSeveralNestedLoops) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "3");
    createCorrectBlock(correctBlocks, "4", 1, "");
    createCorrectBlock(correctBlocks, "5", 0, "10");
    createCorrectBlock(correctBlocks, "7", 1, "");
    createCorrectBlock(correctBlocks, "10", 0, "13");
    createCorrectBlock(correctBlocks, "14", 0, "25");
    createCorrectBlock(correctBlocks, "17", 2, "");
    createCorrectBlock(correctBlocks, "25", 0, "27");
    createCorrectBlock(correctBlocks, "18", 0, "17");
    createCorrectBlock(correctBlocks, "27", 0, "13");
    createCorrectBlock(correctBlocks, "22", 1, "");
    createCorrectBlock(correctBlocks, "31", 2, "");
    createCorrectBlock(correctBlocks, "32", 0, "31");
    createCorrectBlock(correctBlocks, "39", 1, "");
    createCorrectBlock(correctBlocks, "43", 0, "3");
    auto dpsSortedBlocks =
        createTraceAndApplyPhases(&IfOperationFollowedByLoopWithDeeplyNestedIfOperationsWithSeveralNestedLoops_11);
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> emptyIfElse_12() {
    Value agg = Value(0);
    if (agg < 350) {

    } else {
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 12_emptyIfElse) {
    auto dpsSortedBlocks = createTraceAndApplyPhases(&emptyIfElse_12);
    auto convertedIfOperation = dpsSortedBlocks.at(0)->getTerminatorOp();
    ASSERT_EQ(convertedIfOperation->getOperationType(), IR::Operations::Operation::BranchOp);
    auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(dpsSortedBlocks.at(0)->getTerminatorOp());
    ASSERT_EQ(branchOp->getNextBlockInvocation().getBlock()->getIdentifier(), "2");
}

Value<> MergeBlockRightAfterBranchSwitch_13() {
    Value agg = Value(0);
    if (agg < 150) {
        agg = agg + 1;
    } else {
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 13_MergeBlockRightAfterBranchSwitch) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto dpsSortedBlocks = createTraceAndApplyPhases(&MergeBlockRightAfterBranchSwitch_13);
    createCorrectBlock(correctBlocks, "0", 0, "2");
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> StartBlockIsMergeBlock_14() {
    Value agg = Value(0);
    while (agg < 10) {
        if (agg < 150) {
            agg = agg + 1;
        } else {
        }
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 14_StartBlockIsMergeBlock) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto dpsSortedBlocks = createTraceAndApplyPhases(&StartBlockIsMergeBlock_14);
    createCorrectBlock(correctBlocks, "2", 2, "");
    createCorrectBlock(correctBlocks, "3", 0, "2");
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> correctMergeBlockForwardingAfterFindingMergeBlocksOne_15() {
    Value agg = Value(0);
    if (agg < 150) {
        agg = agg + 1;
    } else {
        if (agg < 150) {
            agg = agg + 2;
        } else {
            agg = agg + 3;
        }
        agg = agg + 4;
    }
    return agg;
}
Value<> correctMergeBlockForwardingAfterFindingMergeBlocksTwo_15() {
    Value agg = Value(0);
    if (agg < 150) {
        agg = agg + 1;
    } else {
        if (agg < 150) {
            agg = agg + 2;
        } else {
            agg = agg + 3;
        }
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 15_correctMergeBlockForwardingAfterFindingMergeBlocks) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto dpsSortedBlocks = createTraceAndApplyPhases(&correctMergeBlockForwardingAfterFindingMergeBlocksOne_15);
    createCorrectBlock(correctBlocks, "0", 0, "2");
    createCorrectBlock(correctBlocks, "3", 0, "5");
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
    correctBlocks.clear();
    dpsSortedBlocks = createTraceAndApplyPhases(&correctMergeBlockForwardingAfterFindingMergeBlocksTwo_15);
    createCorrectBlock(correctBlocks, "0", 0, "3");
    createCorrectBlock(correctBlocks, "4", 0, "3");
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> OneMergeBlockThreeIfOperationsFalseBranchIntoTrueBranchIntoFalseBranch_16() {
    Value agg = Value(0);
    if (agg < 50) {
        agg = agg + 1;
    } else {
        if (agg > 60) {
            if (agg > 18) {
                agg = agg + 2;
            } else {
            }
        } else {
            agg = agg + 3;
        }
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 16_OneMergeBlockThreeIfOperationsFalseBranchIntoTrueBranchIntoFalseBranch) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto dpsSortedBlocks = createTraceAndApplyPhases(&OneMergeBlockThreeIfOperationsFalseBranchIntoTrueBranchIntoFalseBranch_16);
    createCorrectBlock(correctBlocks, "0", 0, "4");
    createCorrectBlock(correctBlocks, "5", 0, "4");
    createCorrectBlock(correctBlocks, "6", 0, "4");
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> NestedLoopWithFalseBranchPointingToParentLoopHeader_17() {
    Value agg = Value(0);
    Value limit = Value(10);
    while (agg < limit) {
        if (agg < limit) {
            agg = agg + 1;
        } else {
            for (Value start = 0; start < 10; start = start + 1) {
                agg = agg + 1;
            }
        }
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 17_NestedLoopWithFalseBranchPointingToParentLoopHeader) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto dpsSortedBlocks = createTraceAndApplyPhases(&NestedLoopWithFalseBranchPointingToParentLoopHeader_17);
    createCorrectBlock(correctBlocks, "2", 2, "");
    createCorrectBlock(correctBlocks, "3", 0, "2");
    createCorrectBlock(correctBlocks, "6", 1, "");
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> InterruptedMergeBlockForwarding_18() {
    Value agg = Value(0);
    if (agg < 50) {
        agg = agg + 1;
    } else {
        if (agg > 18) {
            agg = agg + 2;
        } else {
            if (agg > 17) {
                agg = agg + 3;
            }
        }
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 18_InterruptedMergeBlockForwarding) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto dpsSortedBlocks = createTraceAndApplyPhases(&InterruptedMergeBlockForwarding_18);
    createCorrectBlock(correctBlocks, "0", 0, "4");
    createCorrectBlock(correctBlocks, "5", 0, "4");
    createCorrectBlock(correctBlocks, "7", 0, "4");
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> TracingBreaker_19() {
    Value agg = Value(0);
    Value limit = Value(10);
    if (agg < 350) {
        agg = agg + 1;
    }
    if (agg < 350) {
        agg = agg + 1;
    } else {
        if (agg < 350) {
            if (agg < 350) {//the 'false' case of this if this if-operation has no operations -> Block_9
                agg = agg + 1;
            } else {
                agg = agg + 2;// leads to empty block
            }
        }
    }
    return agg;
}
// Breaks in release mode.
TEST_P(StructuredControlFlowPhaseTest, 19_TracingBreaker) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto dpsSortedBlocks = createTraceAndApplyPhases(&TracingBreaker_19);
    createCorrectBlock(correctBlocks, "0", 0, "2");
    createCorrectBlock(correctBlocks, "2", 0, "6");
    createCorrectBlock(correctBlocks, "7", 0, "6");
    createCorrectBlock(correctBlocks, "8", 0, "6");
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}
Value<> DebugVsRelease_20() {
    Value agg = Value(0);
    Value limit = Value(10);
    if (agg < 350) {
        agg = agg + 1;
    }
    if (agg < 350) {
        agg = agg + 1;
    }
    return agg;
}
// Breaks in release mode.
TEST_P(StructuredControlFlowPhaseTest, 20_DebugVsRelease) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto dpsSortedBlocks = createTraceAndApplyPhases(&DebugVsRelease_20);
    createCorrectBlock(correctBlocks, "0", 0, "2");
    createCorrectBlock(correctBlocks, "2", 0, "4");
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

// Tests all registered compilation backends.
// To select a specific compilation backend use ::testing::Values("MLIR") instead of ValuesIn.
auto pluginNames = Backends::CompilationBackendRegistry::getPluginNames();
INSTANTIATE_TEST_CASE_P(testLoopCompilation,
                        StructuredControlFlowPhaseTest,
                        ::testing::ValuesIn(pluginNames.begin(), pluginNames.end()),
                        [](const testing::TestParamInfo<StructuredControlFlowPhaseTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus