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

#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/SymbolicExecution/SymbolicExecutionContext.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
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
class StructuredControlFlowPhaseTest : public testing::Test, public AbstractCompilationBackendTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TraceTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup TraceTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TraceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TraceTest test class." << std::endl; }

    // Takes a Nautilus function, creates the trace, converts it Nautilus IR, and applies all available phases.
    std::vector<IR::OwningBasicBlockPtr> createTraceAndApplyPhases(std::function<Value<>()> nautilusFunction) {
        auto execution = Nautilus::Tracing::traceFunctionWithReturn([nautilusFunction]() {
            return nautilusFunction();
        });
        auto executionTrace = ssaCreationPhase.apply(std::move(execution));
        auto ir = irCreationPhase.apply(executionTrace);
        auto dpsSortedGraphNodes = enumerateIRForTests(ir);
        removeBrOnlyBlocksPhase.apply(ir);
        loopDetectionPhase.apply(ir);
        structuredControlFlowPhase.apply(ir);
        return dpsSortedGraphNodes;
    }

    /**
     * @brief lowerBound, upperBound, stepSize, loopEndBlockId
     */
    struct CountedLoopInfo {
        uint32_t lowerBound;
        uint32_t upperBound;
        uint32_t stepSize;
        std::string loopEndBlockId;
    };
    using countedLoopInfoPtr = std::unique_ptr<CountedLoopInfo>;
    /**
     * @brief Create correctCountedLoopInfo.
     * 
     * @param lowerBound: The lower bound of the counted loop (minimum: 0)
     * @param upperBound: The upper bound of the counted loop
     * @param stepSize: The step size by which the loop induction variable is increased in each step.
     * @param loopEndBlockId: The id of the block that loops back to the loop-header, and passes the final values.
     * @return countedLoopInfoPtr
     */
    countedLoopInfoPtr
    createCorrectCountedLoopInfo(uint32_t lowerBound, uint32_t upperBound, uint32_t stepSize, std::string loopEndBlockId) {
        return std::make_unique<CountedLoopInfo>(CountedLoopInfo{lowerBound, upperBound, stepSize, loopEndBlockId});
    }

    struct CorrectBlockValues {
        uint32_t correctNumberOfLoopBackEdges;
        std::unique_ptr<CountedLoopInfo> countedLoopInfo;
        std::string correctMergeBlockId;
    };
    using CorrectBlockValuesPtr = std::unique_ptr<CorrectBlockValues>;
    /**
     * @brief Create a CorrectBlock, which holds the expected data for exactly one block.
     * 
     * @param correctBlocks: A list into which we push the newly created 'correctBlock'
     * @param correctBlockId: The id of the block that we create the expected to be correct values for.
     * @param correctNumberOfLoopBackEdges: The number of expected loop back edges.
     * @param correctMergeBlockId: The expected merge-block id (if block contains if-operation).
     * @param countedLoopInfo: The expected countedLoopInfo(lower- and upperBound, stepSize, loopEndBlockId)
     */
    void createCorrectBlock(std::unordered_map<std::string, CorrectBlockValuesPtr>& correctBlocks,
                            std::string correctBlockId,
                            uint32_t correctNumberOfLoopBackEdges,
                            std::string correctMergeBlockId,
                            countedLoopInfoPtr countedLoopInfo = nullptr) {
        correctBlocks.emplace(
            std::pair{correctBlockId,
                      std::make_unique<CorrectBlockValues>(
                          CorrectBlockValues{correctNumberOfLoopBackEdges, std::move(countedLoopInfo), correctMergeBlockId})});
    }

    /**
     * @brief Takes a graph IR, enumerates all blocks depth-first, and returns a vector containing all blocks.
     * 
     * @param ir: Graph ir that is traversed depth-first-search (dps).
     * @return std::vector<IR::BasicBlockPtr>: A vector containing all blocks of the ir-graph sorted by dps.
     */
    std::vector<IR::OwningBasicBlockPtr> enumerateIRForTests(std::shared_ptr<IR::IRGraph> ir) {
        std::stack<IR::BasicBlock*> newBlocks;
        std::unordered_set<IR::BasicBlock*> visitedBlocks;
        std::vector<IR::OwningBasicBlockPtr> dpsSortedIRGraph;

        uint32_t currentId = 0;
        newBlocks.emplace(ir->getRootOperation().getFunctionBasicBlock());
        do {
            visitedBlocks.emplace(newBlocks.top());
            newBlocks.top()->setIdentifier(std::to_string(currentId));
            ++currentId;
            dpsSortedIRGraph.emplace_back(newBlocks.top());
            auto nextBlocks = newBlocks.top()->getNextBlocks();
            newBlocks.pop();
            if (nextBlocks.second && !visitedBlocks.contains(nextBlocks.first)) {
                newBlocks.emplace(nextBlocks.second);
            }
            if (nextBlocks.first && !visitedBlocks.contains(nextBlocks.first)) {
                newBlocks.emplace(nextBlocks.first);
            }
        } while (!newBlocks.empty());
        return dpsSortedIRGraph;
    }

    /**
     * @brief Takes a list of blocks, and a map that maps the basic block identifiers to a data structure that 
     *          holds the expected, correct values for the respective block. The function iterates over all blocks,
     *          and checks whether the basic blocks reflect the expected values.
     * 
     * @param dpsSortedBlocks An IR graph represented as a list of blocks (nodes).
     * @param correctBlocks A map that maps blocks to a data structure that holds the expected values for the blocks.
     * @return true, if all values in all blocks met the expected values, else, return false.
     */
    bool checkIRForCorrectness(const std::vector<IR::OwningBasicBlockPtr>& dpsSortedBlocks,
                               const std::unordered_map<std::string, CorrectBlockValuesPtr>& correctBlocks) {
        bool mergeBlocksAreCorrect = true;
        bool loopInfoIsCorrect = true;
        bool backLinksAreCorrect = true;
        uint32_t numCheckedBlocks = 0;
        for (auto currentBlock : dpsSortedBlocks) {
            if (currentBlock->getTerminatorOp().getOperationType() == IR::Operations::Operation::OperationType::IfOp) {
                // Check that the currentBlock is actually part of the solution set.
                if (correctBlocks.contains(currentBlock->getIdentifier())) {
                    auto& ifOp = dynamic_cast<IR::Operations::IfOperation&>(currentBlock->getTerminatorOp());
                    // Check that the merge-block id is set correctly, if the if-operation has a merge-block.
                    auto correctMergeBlockId = correctBlocks.at(currentBlock->getIdentifier())->correctMergeBlockId;
                    if (!correctMergeBlockId.empty()) {
                        if (!ifOp.getMergeBlock()) {
                            NES_ERROR("CurrentBlock: {} did not contain a merge block even though the solution suggest it has a "
                                      "merge-block with id: {}",
                                      currentBlock->getIdentifier(),
                                      correctMergeBlockId);
                            mergeBlocksAreCorrect = false;
                        } else {
                            bool correctMergeBlock = ifOp.getMergeBlock()->getIdentifier() == correctMergeBlockId;
                            mergeBlocksAreCorrect &= correctMergeBlock;
                            if (!correctMergeBlock) {
                                NES_ERROR("\nMerge-Block mismatch for block {}: {} instead of {} (correct).",
                                          currentBlock->getIdentifier(),
                                          ifOp.getMergeBlock()->getIdentifier(),
                                          correctBlocks.at(currentBlock->getIdentifier())->correctMergeBlockId);
                            }
                        }
                    } else {
                        bool noMergeBlockCorrectlySet = !ifOp.getMergeBlock();
                        mergeBlocksAreCorrect &= noMergeBlockCorrectlySet;
                        if (!noMergeBlockCorrectlySet) {
                            NES_ERROR("The current merge block: {} contains a merge-block with id: {}, even though it should "
                                      "not contain a merge-block.",
                                      currentBlock->getIdentifier(),
                                      ifOp.getMergeBlock()->getIdentifier());
                        }
                    }
                } else {
                    mergeBlocksAreCorrect = false;
                    NES_ERROR(
                        "CurrentBlock with id: {} was not part of solution set(correctBlocks), but it contains an if-operation.",
                        currentBlock->getIdentifier());
                }
                ++numCheckedBlocks;
            } else if (currentBlock->getTerminatorOp().getOperationType() == IR::Operations::Operation::OperationType::LoopOp) {
                auto& loopOp = dynamic_cast<IR::Operations::LoopOperation&>(currentBlock->getTerminatorOp());
                // Check loop operation for correctness.
                if (correctBlocks.contains(currentBlock->getIdentifier())) {
                    if (correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo) {
                        loopInfoIsCorrect = loopOp.getLoopInfo() != nullptr;
                        if (!loopInfoIsCorrect) {
                            NES_ERROR("Loop operation in block: {} should -not- contain counted loop info.",
                                      currentBlock->getIdentifier());
                        } else {
                            auto countedLoopInfo =
                                std::static_pointer_cast<IR::Operations::CountedLoopInfo>(loopOp.getLoopInfo());
                            loopInfoIsCorrect &= countedLoopInfo->lowerBound
                                == correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->lowerBound;
                            loopInfoIsCorrect &= countedLoopInfo->upperBound
                                == correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->upperBound;
                            loopInfoIsCorrect &= countedLoopInfo->stepSize
                                == correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->stepSize;
                            loopInfoIsCorrect &= loopOp.getLoopEndBlock().getBlock()->getIdentifier()
                                == correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->loopEndBlockId;
                            if (!loopInfoIsCorrect) {
                                NES_ERROR("Loop info set incorrectly. Check values: LowerBound: {} vs {}, UpperBound: {} vs {}, "
                                          "StepSize: {} vs {}, LoopEndBlock: {} vs {}",
                                          countedLoopInfo->lowerBound,
                                          correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->lowerBound,
                                          countedLoopInfo->upperBound,
                                          correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->upperBound,
                                          countedLoopInfo->stepSize,
                                          correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->stepSize,
                                          loopOp.getLoopEndBlock().getBlock()->getIdentifier(),
                                          correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->loopEndBlockId);
                            }
                        }
                    } else {
                        bool loopIsDefaultLoop = (loopOp.getLoopType() == IR::Operations::LoopOperation::LoopType::DefaultLoop);
                        loopInfoIsCorrect &= loopIsDefaultLoop;
                        if (!loopIsDefaultLoop) {
                            NES_ERROR("\n Loop operation in block: {} should be default loop, but is not.",
                                      currentBlock->getIdentifier());
                        }
                    }
                    // Check that the number of loop back edges is set correctly.
                    backLinksAreCorrect &= currentBlock->getNumLoopBackEdges()
                        == correctBlocks.at(currentBlock->getIdentifier())->correctNumberOfLoopBackEdges;
                    if (!backLinksAreCorrect) {
                        NES_ERROR("\nBlock -{}- contained -{}- backLinks instead of: -{}-.",
                                  currentBlock->getIdentifier(),
                                  currentBlock->getNumLoopBackEdges(),
                                  correctBlocks.at(currentBlock->getIdentifier())->correctNumberOfLoopBackEdges);
                    }
                } else {
                    mergeBlocksAreCorrect = false;
                    NES_ERROR(
                        "CurrentBlock with id: {} was not part of solution set(correctBlocks), but it contains a loop-operation.",
                        currentBlock->getIdentifier());
                }
                ++numCheckedBlocks;
            } else {
                if (correctBlocks.contains(currentBlock->getIdentifier())) {
                    mergeBlocksAreCorrect = false;
                    NES_ERROR("CurrentBlock with id: {} was part of solution set(correctBlocks), but it does not contain an "
                              "if-operation.",
                              currentBlock->getIdentifier());
                }
            }
        }
        bool numCheckedBlocksMatchesNumCorrectBlocks = numCheckedBlocks == correctBlocks.size();
        if (!numCheckedBlocksMatchesNumCorrectBlocks) {
            NES_ERROR("The number of checked IR blocks {} does not match the number of given 'correctBlocks': {}",
                      numCheckedBlocks,
                      correctBlocks.size());
        }
        return mergeBlocksAreCorrect && loopInfoIsCorrect && backLinksAreCorrect;
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
    Value agg2 = Value(0);
    if (agg < 50) {
        if (agg < 40) {
            agg2 = agg2 + 10;
        } else {
            agg = agg + 100;
        }
        if (agg > 60) {
            agg = agg + 1000;
        } else {
            agg2 = agg2 + 10000;
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
    Value limit = Value(10);
    for (Value j = 0; j < limit; j = j + 1) {
        agg = agg + 1;
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 5_oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsFollowedUpByLoopHeader) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "5");
    createCorrectBlock(correctBlocks, "1", 0, "5");
    createCorrectBlock(correctBlocks, "6", 1, "", createCorrectCountedLoopInfo(0, 10, 1, "7"));
    createCorrectBlock(correctBlocks, "9", 0, "5");
    auto dpsSortedBlocks =
        createTraceAndApplyPhases(&oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsFollowedUpByLoopHeader_5);
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsLoopHeader_6() {
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
    Value start = Value(1);
    Value limit = Value(1000000);
    while (start < limit) {
        agg = agg + 3;
        start = start + 2;
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 6_oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsLoopHeader) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "5");
    createCorrectBlock(correctBlocks, "1", 0, "5");
    createCorrectBlock(correctBlocks, "6", 1, "", createCorrectCountedLoopInfo(1, 1000000, 2, "7"));
    createCorrectBlock(correctBlocks, "9", 0, "5");
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
    Value start = Value(0);
    Value limit2 = Value(1000);
    while (start < limit2) {
        agg = agg + 4;
        start = start + 1;
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 8_mergeLoopMergeBlockWithLoopFollowUp) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "4");
    createCorrectBlock(correctBlocks, "4", 2, "");
    createCorrectBlock(correctBlocks, "5", 0, "4");
    createCorrectBlock(correctBlocks, "9", 1, "", createCorrectCountedLoopInfo(0, 1000, 1, "10"));
    auto dpsSortedBlocks = createTraceAndApplyPhases(&mergeLoopMergeBlockWithLoopFollowUp_8);
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> LoopHeaderWithNineBackLinks_9() {
    Value agg = Value(0);
    Value upperBound1 = Value(1000);
    while (agg < upperBound1) {
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
    Value inductionVar2 = Value(0);
    Value upperBound2 = Value(1000);
    while (inductionVar2 < upperBound2) {
        agg = agg + 4;
        inductionVar2 = inductionVar2 + 2;
    }
    return agg;
}
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
    createCorrectBlock(correctBlocks, "34", 1, "", createCorrectCountedLoopInfo(0, 1000, 2, "35"));
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
            for (Value start = 0; start < 100; start = start + 1) {
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
                for (Value start = 0; start < 1000; start = start + 1) {
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
    createCorrectBlock(correctBlocks, "4", 1, "", createCorrectCountedLoopInfo(0, 10, 1, "13"));
    createCorrectBlock(correctBlocks, "5", 0, "10");
    createCorrectBlock(correctBlocks, "7", 1, "");
    createCorrectBlock(correctBlocks, "10", 0, "13");
    createCorrectBlock(correctBlocks, "14", 0, "25");
    createCorrectBlock(correctBlocks, "17", 2, "");
    createCorrectBlock(correctBlocks, "25", 0, "27");
    createCorrectBlock(correctBlocks, "18", 0, "17");
    createCorrectBlock(correctBlocks, "27", 0, "13");
    createCorrectBlock(correctBlocks, "22", 1, "", createCorrectCountedLoopInfo(0, 1000, 1, "23"));
    createCorrectBlock(correctBlocks, "31", 2, "");
    createCorrectBlock(correctBlocks, "32", 0, "31");
    createCorrectBlock(correctBlocks, "39", 1, "", createCorrectCountedLoopInfo(0, 100, 1, "40"));
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
    auto& convertedIfOperation = dpsSortedBlocks.at(0)->getTerminatorOp();
    ASSERT_EQ(convertedIfOperation.getOperationType(), IR::Operations::Operation::OperationType::BranchOp);
    auto& branchOp = dynamic_cast<IR::Operations::BranchOperation&>(dpsSortedBlocks.at(0)->getTerminatorOp());
    ASSERT_EQ(branchOp.getNextBlockInvocation().getBlock()->getIdentifier(), "2");
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
    createCorrectBlock(correctBlocks, "6", 1, "", createCorrectCountedLoopInfo(0, 10, 1, "7"));
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
TEST_P(StructuredControlFlowPhaseTest, 20_DebugVsRelease) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto dpsSortedBlocks = createTraceAndApplyPhases(&DebugVsRelease_20);
    createCorrectBlock(correctBlocks, "0", 0, "2");
    createCorrectBlock(correctBlocks, "2", 0, "4");
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> ExtensiveCountedLoopDetection_21() {
    Value agg = Value(2);
    // 1. Standard for loop.
    for (Value lowerBound = Value(0); lowerBound < 10; lowerBound = lowerBound + 1) {
        agg = agg + 1;
    }
    // 2. for loop, but condition is flipped.
    for (Value lowerBound = Value(0); 10 > lowerBound; lowerBound = lowerBound + 2) {
        agg = agg + 1;
    }
    // 3. for loop, but addition order is flipped.
    for (Value lowerBound = Value(0); lowerBound < 10; lowerBound = 3 + lowerBound) {
        agg = agg + 1;
    }
    // 4. for loop, but condition, and addition order are flipped.
    for (Value lowerBound = Value(0); 10 > lowerBound; lowerBound = 4 + lowerBound) {
        agg = agg + 1;
    }
    // 5. for loop, but the induction variable is defined outside of the loop.
    Value lowerBound_5 = Value(0);
    for (; lowerBound_5 < 10; lowerBound_5 = lowerBound_5 + 5) {
        agg = agg + 1;
    }
    // 6. for loop, but the induction variable, and the upperBound are defined outside of the loop.
    Value lowerBound_6 = Value(0);
    Value upperBound_6 = Value(10);
    for (; lowerBound_6 < upperBound_6; lowerBound_6 = lowerBound_6 + 6) {
        agg = agg + 1;
    }
    // 7. for loop, but the induction variable, and the upperBound are defined outside of the loop,
    //    and the condition and add operations are flipped.
    Value lowerBound_7 = Value(0);
    Value upperBound_7 = Value(10);
    for (; upperBound_7 > lowerBound_7; lowerBound_7 = 7 + lowerBound_7) {
        agg = agg + 1;
    }
    // 8. while loop counted
    Value lowerBound_8 = Value(0);
    while (lowerBound_8 < 10) {
        agg = agg + 1;
        lowerBound_8 = lowerBound_8 + 8;
    }
    // 9. while loop counted, but upperBound is defined outside
    Value lowerBound_9 = Value(0);
    Value upperBound_9 = Value(10);
    while (lowerBound_9 < upperBound_9) {
        agg = agg + 1;
        lowerBound_9 = lowerBound_9 + 9;
    }
    // 10. while loop counted, but upperBound is defined outside, and condition is flipped
    Value lowerBound_10 = Value(0);
    Value upperBound_10 = Value(10);
    while (upperBound_10 > lowerBound_10) {
        agg = agg + 1;
        lowerBound_10 = lowerBound_10 + 10;
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 21_ExtensiveCountedLoopDetection) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto dpsSortedBlocks = createTraceAndApplyPhases(&ExtensiveCountedLoopDetection_21);
    createCorrectBlock(correctBlocks, "1", 1, "", createCorrectCountedLoopInfo(0, 10, 1, "2"));
    createCorrectBlock(correctBlocks, "4", 1, "", createCorrectCountedLoopInfo(0, 10, 2, "5"));
    createCorrectBlock(correctBlocks, "7", 1, "", createCorrectCountedLoopInfo(0, 10, 3, "8"));
    createCorrectBlock(correctBlocks, "10", 1, "", createCorrectCountedLoopInfo(0, 10, 4, "11"));
    createCorrectBlock(correctBlocks, "13", 1, "", createCorrectCountedLoopInfo(0, 10, 5, "14"));
    createCorrectBlock(correctBlocks, "16", 1, "", createCorrectCountedLoopInfo(0, 10, 6, "17"));
    createCorrectBlock(correctBlocks, "19", 1, "", createCorrectCountedLoopInfo(0, 10, 7, "20"));
    createCorrectBlock(correctBlocks, "22", 1, "", createCorrectCountedLoopInfo(0, 10, 8, "23"));
    createCorrectBlock(correctBlocks, "25", 1, "", createCorrectCountedLoopInfo(0, 10, 9, "26"));
    createCorrectBlock(correctBlocks, "28", 1, "", createCorrectCountedLoopInfo(0, 10, 10, "29"));
    // createCorrectBlock(correctBlocks, "31", 1, "");
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> NonCountedLoopCases_22() {
    Value agg = Value(2);
    // 1. while loop counted, but condition for lowerBound vs upperBound is wrong.
    Value lowerBound_1 = Value(0);
    Value upperBound_1 = Value(10);
    while (upperBound_1 < lowerBound_1) {
        agg = agg + 1;
        lowerBound_1 = lowerBound_1 + 1;
    }
    // 2. while loop counted, but an old value is used in the loop condition.
    Value lowerBound_2 = Value(0);
    Value upperBound_2 = Value(10);
    while (lowerBound_1 < upperBound_2) {//<- using lowerBound_1, which is passed through several blocks before.
        agg = agg + 1;
        lowerBound_2 = lowerBound_2 + 2;
    }
    // 3. while loop counted, but the stepSize is not defined in the loopEndBlock.
    Value lowerBound_3 = Value(0);
    Value upperBound_3 = Value(10);
    Value stepSize_3 = Value(1);
    while (lowerBound_3 < upperBound_3) {
        agg = agg + 1;
        lowerBound_3 = lowerBound_3 + stepSize_3;
    }
    // 4. for loop, but the inductionVar is decreased.
    Value lowerBound = Value(10);
    for (; lowerBound > 0; lowerBound = lowerBound - 1) {
        agg = agg + 1;
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 22_ExtensiveCountedLoopDetection) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto dpsSortedBlocks = createTraceAndApplyPhases(&NonCountedLoopCases_22);
    createCorrectBlock(correctBlocks, "1", 1, "");
    createCorrectBlock(correctBlocks, "4", 1, "");
    createCorrectBlock(correctBlocks, "7", 1, "");
    createCorrectBlock(correctBlocks, "10", 1, "");
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> twoCountedLoopsOneLargeOneSmall_23() {
    Value agg = Value(0);
    Value upperBound1 = Value(1000);
    for (Value inductionVar1 = Value(0); inductionVar1 < upperBound1; inductionVar1 = inductionVar1 + 1) {
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
    Value inductionVar2 = Value(0);
    Value upperBound2 = Value(1000);
    while (inductionVar2 < upperBound2) {
        agg = agg + 4;
        inductionVar2 = inductionVar2 + 2;
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, twoCountedLoopsOneLargeOneSmall) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "1", 1, "", createCorrectCountedLoopInfo(0, 1000, 1, "15"));
    createCorrectBlock(correctBlocks, "2", 0, "15");
    createCorrectBlock(correctBlocks, "3", 0, "15");
    createCorrectBlock(correctBlocks, "4", 0, "6");
    createCorrectBlock(correctBlocks, "6", 0, "15");

    createCorrectBlock(correctBlocks, "16", 0, "18");
    createCorrectBlock(correctBlocks, "22", 0, "15");
    createCorrectBlock(correctBlocks, "23", 0, "15");
    createCorrectBlock(correctBlocks, "24", 0, "15");
    createCorrectBlock(correctBlocks, "28", 0, "15");
    createCorrectBlock(correctBlocks, "30", 0, "15");
    createCorrectBlock(correctBlocks, "34", 1, "", createCorrectCountedLoopInfo(0, 1000, 2, "35"));
    auto dpsSortedBlocks = createTraceAndApplyPhases(&twoCountedLoopsOneLargeOneSmall_23);
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

Value<> DetectCountedLoopsWithEqualInComparison_23() {
    Value agg = Value(2);
    // 1. Counted for loop with smaller equals in condition.
    for (Value inductionVar_1 = Value(0); inductionVar_1 <= 10; inductionVar_1 = inductionVar_1 + 1) {
        agg = agg + 1;
    }
    // 2. Counted for loop with smaller equals in condition, but condition is flipped.
    Value inductionVar_1 = Value(0);
    Value upperBound_2 = Value(10);
    for (; upperBound_2 >= inductionVar_1; inductionVar_1 = inductionVar_1 + 2) {
        agg = agg + 1;
    }
    // 3. Counted while loop with smaller equals in condition.
    Value inductionVar_3 = Value(0);
    Value upperBound_3 = Value(10);
    while (inductionVar_3 <= upperBound_3) {
        agg = agg + 4;
        inductionVar_3 = inductionVar_3 + 3;
    }
    // 4. Counted for loop with smaller equals condition, but condition is complex (not supported).
    Value inductionVar_4 = Value(0);
    Value otherBound_4 = Value(1);
    for (; (inductionVar_4 + 10) <= (inductionVar_4 - otherBound_4); inductionVar_4 = inductionVar_4 + 1) {
        agg = agg + 1;
    }
    // 5. Smaller equals, but the comparison order of '<=', which is (<,==,or) is faked.
    Value otherBound_5 = Value(1);
    for (Value lowerBound = Value(0); lowerBound < 10 || lowerBound == otherBound_5; lowerBound = lowerBound + 1) {
        agg = agg + 1;
    }
    // 6. Counted for loop not equals(!=) in condition (not supported).
    for (Value inductionVar_6 = Value(0); inductionVar_6 != 10; inductionVar_6 = inductionVar_6 + 1) {
        agg = agg + 1;
    }
    // 7. Counted for loop with equals (==) in condition (not supported).
    for (Value inductionVar_8 = Value(0); inductionVar_8 == 10; inductionVar_8 = inductionVar_8 + 1) {
        agg = agg + 1;
    }
    return agg;
}
TEST_P(StructuredControlFlowPhaseTest, 23_DetectCountedLoopsWithEqualInComparison) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto dpsSortedBlocks = createTraceAndApplyPhases(&DetectCountedLoopsWithEqualInComparison_23);
    createCorrectBlock(correctBlocks, "1", 1, "", createCorrectCountedLoopInfo(0, 11, 1, "2"));
    createCorrectBlock(correctBlocks, "4", 1, "", createCorrectCountedLoopInfo(0, 11, 2, "5"));
    createCorrectBlock(correctBlocks, "7", 1, "", createCorrectCountedLoopInfo(0, 11, 3, "8"));
    createCorrectBlock(correctBlocks, "10", 1, "");
    createCorrectBlock(correctBlocks, "13", 1, "");
    createCorrectBlock(correctBlocks, "16", 1, "");
    createCorrectBlock(correctBlocks, "19", 1, "");
    ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

// Tests all registered compilation backends.
// To select a specific compilation backend use ::testing::Values("MLIR") instead of ValuesIn.
INSTANTIATE_TEST_CASE_P(testLoopCompilation,
                        StructuredControlFlowPhaseTest,
                        ::testing::ValuesIn(Backends::CompilationBackendRegistry::getPluginNames().begin(),
                                            Backends::CompilationBackendRegistry::getPluginNames().end()),
                        [](const testing::TestParamInfo<StructuredControlFlowPhaseTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus