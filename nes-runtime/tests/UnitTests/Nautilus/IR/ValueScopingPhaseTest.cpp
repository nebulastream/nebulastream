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
#include <Nautilus/IR/Phases/ValueScopingPhase.hpp>
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
 * @brief Todo
 */
class ValueScopingPhaseTest : public testing::Test, public AbstractCompilationBackendTest {
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
    std::vector<IR::BasicBlockPtr> createTraceAndApplyPhases(std::function<Value<>()> nautilusFunction) {
        auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([nautilusFunction]() {
            return nautilusFunction();
        });
        auto executionTrace = ssaCreationPhase.apply(std::move(execution));
        auto ir = irCreationPhase.apply(executionTrace);
        // auto dpsSortedGraphNodes = enumerateIRForTests(ir);
        removeBrOnlyBlocksPhase.apply(ir);
        loopDetectionPhase.applyLoopDetection(ir);
        structuredControlFlowPhase.apply(ir);
        // return enumerateIRForTests(ir);
        valueScopingPhase.apply(ir);
        // return dpsSortedGraphNodes;
        return enumerateIRForTests(ir);
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
    countedLoopInfoPtr
    createCorrectCountedLoopInfo(uint32_t lowerBound, uint32_t upperBound, uint32_t stepSize, std::string loopEndBlockId) {
        return std::make_unique<CountedLoopInfo>(CountedLoopInfo{lowerBound, upperBound, stepSize, loopEndBlockId});
    }

    struct CorrectBlockValues {
        uint32_t correctNumberOfBackLinks;
        std::unique_ptr<CountedLoopInfo> countedLoopInfo;
        std::string correctMergeBlockId;
    };
    using CorrectBlockValuesPtr = std::unique_ptr<CorrectBlockValues>;
    void createCorrectBlock(std::unordered_map<std::string, CorrectBlockValuesPtr>& correctBlocks,
                            std::string correctBlockId,
                            uint32_t correctNumberOfBackLinks,
                            std::string correctMergeBlockId,
                            countedLoopInfoPtr countedLoopInfo = nullptr) {
        correctBlocks.emplace(
            std::pair{correctBlockId,
                      std::make_unique<CorrectBlockValues>(
                          CorrectBlockValues{correctNumberOfBackLinks, std::move(countedLoopInfo), correctMergeBlockId})});
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
        bool loopInfoIsCorrect = true;
        bool backLinksAreCorrect = true;
        uint32_t numCheckedBlocks = 0;
        for (auto currentBlock : dpsSortedBlocks) {
            if (currentBlock->getTerminatorOp()->getOperationType() == IR::Operations::Operation::IfOp) {
                // Check that the currentBlock is actually part of the solution set.
                if (correctBlocks.contains(currentBlock->getIdentifier())) {
                    auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(currentBlock->getTerminatorOp());
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
                ++numCheckedBlocks;
            } else if (currentBlock->getTerminatorOp()->getOperationType() == IR::Operations::Operation::LoopOp) {
                auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(currentBlock->getTerminatorOp());
                // Check loop operation for correctness.
                if (correctBlocks.contains(currentBlock->getIdentifier())) {
                    if (correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo) {
                        loopInfoIsCorrect = loopOp->getLoopInfo() != nullptr;
                        if (!loopInfoIsCorrect) {
                            NES_ERROR("Loop operation in block: " << currentBlock->getIdentifier()
                                                                  << " should -not- contain counted loop info.");
                        } else {
                            auto countedLoopInfo =
                                std::static_pointer_cast<IR::Operations::CountedLoopInfo>(loopOp->getLoopInfo());
                            loopInfoIsCorrect &= countedLoopInfo->lowerBound
                                == correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->lowerBound;
                            loopInfoIsCorrect &= countedLoopInfo->upperBound
                                == correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->upperBound;
                            loopInfoIsCorrect &= countedLoopInfo->stepSize
                                == correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->stepSize;
                            loopInfoIsCorrect &= loopOp->getLoopEndBlock().getNextBlock()->getIdentifier()
                                == correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->loopEndBlockId;
                            if (!loopInfoIsCorrect) {
                                NES_ERROR("Loop info set incorrectly. Check values: "
                                          << "LowerBound: " << countedLoopInfo->lowerBound << " vs "
                                          << correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->lowerBound
                                          << ", UpperBound: " << countedLoopInfo->upperBound << " vs "
                                          << correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->upperBound
                                          << ", StepSize: " << countedLoopInfo->stepSize << " vs "
                                          << correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->stepSize
                                          << ", LoopEndBlock: " << loopOp->getLoopEndBlock().getNextBlock()->getIdentifier()
                                          << " vs "
                                          << correctBlocks.at(currentBlock->getIdentifier())->countedLoopInfo->loopEndBlockId);
                            }
                        }
                    } else {
                        bool loopIsDefaultLoop = (loopOp->getLoopType() == IR::Operations::LoopOperation::LoopType::DefaultLoop);
                        loopInfoIsCorrect &= loopIsDefaultLoop;
                        if (!loopIsDefaultLoop) {
                            NES_ERROR("\n Loop operation in block: " << currentBlock->getIdentifier()
                                                                     << " should be default loop, but is not.");
                        }
                    }
                    // Check that the number of loop back edges is set correctly.
                    backLinksAreCorrect &= currentBlock->getNumLoopBackEdges()
                        == correctBlocks.at(currentBlock->getIdentifier())->correctNumberOfBackLinks;
                    if (!backLinksAreCorrect) {
                        NES_ERROR("\nBlock -" << currentBlock->getIdentifier() << "- contained -"
                                              << currentBlock->getNumLoopBackEdges() << "- backLinks instead of: -"
                                              << correctBlocks.at(currentBlock->getIdentifier())->correctNumberOfBackLinks
                                              << "-.");
                    }
                } else {
                    mergeBlocksAreCorrect = false;
                    NES_ERROR("CurrentBlock with id: "
                              << currentBlock->getIdentifier()
                              << " was not part of solution set(correctBlocks), but it contains a loop-operation.");
                }
                ++numCheckedBlocks;
            } else {
                if (correctBlocks.contains(currentBlock->getIdentifier())) {
                    mergeBlocksAreCorrect = false;
                    NES_ERROR("CurrentBlock with id: "
                              << currentBlock->getIdentifier()
                              << " was part of solution set(correctBlocks), but it does not contain an if-operation.");
                }
            }
        }
        bool numCheckedBlocksMatchesNumCorrectBlocks = numCheckedBlocks == correctBlocks.size();
        if (!numCheckedBlocksMatchesNumCorrectBlocks) {
            NES_ERROR("The number of checked IR blocks "
                      << numCheckedBlocks << " does not match the number of given 'correctBlocks': " << correctBlocks.size());
        }
        return mergeBlocksAreCorrect && loopInfoIsCorrect && backLinksAreCorrect;
    }
};

//==----------------------------------------------------------==//
//==------------------ NAUTILUS PHASE TESTS ------------------==//
//==----------------------------------------------------------==//
Value<> oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsFollowedUpByLoopHeader_5() {
    // Value agg = Value(0);
    // Value agg2 = Value(2);
    // Value limit = Value(3);
    // // for (Value inductionVar_1 = Value(0); inductionVar_1 <= 10; inductionVar_1 = inductionVar_1 + 1) {
    // //     agg = agg + 1;
    // // }
    // if(agg < 10) {
    //     if(agg < limit) {
    //         agg = agg + 1;
    //     } else {
    //         agg = agg + 4;
    //     }
    // } else {
    //     agg = agg + 2;
    //     // for (Value inductionVar_1 = Value(0); inductionVar_1 <= 10; inductionVar_1 = inductionVar_1 + 1) {
    //     //     if(agg < 10) {
    //     //         agg = agg + 1;
    //     //     }
    //     // }
    // }
    // if(agg < limit) {
    //     agg = agg + 3;
    // } else {
    //     agg = agg + 4;
    // }
    // return agg + agg2;
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
    // Value start = Value(1);
    // Value limit = Value(1000000);
    while (agg < 10) {
        agg = agg + 3;
        // start = start + 2;
    }
    return agg;
}
TEST_P(ValueScopingPhaseTest, 5_oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsFollowedUpByLoopHeader) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    // createCorrectBlock(correctBlocks, "0", 0, "5");
    // createCorrectBlock(correctBlocks, "1", 1, "", createCorrectCountedLoopInfo(0, 10, 1, "7"));
    // createCorrectBlock(correctBlocks, "9", 0, "5");
    auto dpsSortedBlocks =
        createTraceAndApplyPhases(&oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsFollowedUpByLoopHeader_5);
    // ASSERT_EQ(checkIRForCorrectness(dpsSortedBlocks, correctBlocks), true);
}

// Tests all registered compilation backends.
// To select a specific compilation backend use ::testing::Values("MLIR") instead of ValuesIn.
auto pluginNames = Backends::CompilationBackendRegistry::getPluginNames();
INSTANTIATE_TEST_CASE_P(testLoopCompilation,
                        ValueScopingPhaseTest,
                        ::testing::ValuesIn(pluginNames.begin(), pluginNames.end()),
                        [](const testing::TestParamInfo<ValueScopingPhaseTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus