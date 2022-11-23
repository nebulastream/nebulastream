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

#include "Nautilus/Interface/FunctionCall.hpp"
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Runtime/BufferManager.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <unordered_map>


namespace NES::Nautilus {

class StructuredControlFlowPassTest : public testing::Test, public AbstractCompilationBackendTest {
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

    // Takes a Nautilus function, creates the trace, converts it Nautilus IR, and applies all available passes.
    std::shared_ptr<NES::Nautilus::IR::IRGraph> createTraceAndApplyPasses(std::function<Value<>()> nautilusFunction, 
            bool findSimpleCountedLoops = false) {
        auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([nautilusFunction]() {
            return nautilusFunction();
        });
        auto executionTrace = ssaCreationPhase.apply(std::move(execution));
        auto ir = irCreationPhase.apply(executionTrace);
        ir = removeBrOnlyBlocksPass.apply(ir);
        return structuredControlFlowPass.apply(ir, findSimpleCountedLoops);
    }

    struct CorrectBlockValues {
        uint32_t correctNumberOfBackLinks;
        std::string correctMergeBlockId;
    };
    using CorrectBlockValuesPtr = std::unique_ptr<CorrectBlockValues>;
    void createCorrectBlock(std::unordered_map<std::string, CorrectBlockValuesPtr>& correctBlocks, 
            std::string correctBlockId, uint32_t correctNumberOfBackLinks, std::string correctMergeBlockId) {
        correctBlocks.emplace(std::pair{correctBlockId, std::make_unique<CorrectBlockValues>(CorrectBlockValues{correctNumberOfBackLinks, correctMergeBlockId})});
    }

    bool checkIRForCorrectness(IR::BasicBlockPtr currentBlock, const std::unordered_map<std::string, CorrectBlockValuesPtr>& correctBlocks) {
        std::vector<IR::BasicBlockPtr> candidates;
        std::unordered_set<std::string> visitedBlocks;
        candidates.push_back(currentBlock);
        bool mergeBlocksAreCorrect = true;
        bool backLinksAreCorrect = true;
        do {
            visitedBlocks.emplace(currentBlock->getIdentifier());
            currentBlock = candidates.back();
            if(correctBlocks.contains(currentBlock->getIdentifier())) {
                auto ifOp =  std::static_pointer_cast<IR::Operations::IfOperation>(currentBlock->getTerminatorOp());
                backLinksAreCorrect = currentBlock->getNumLoopBackEdges() == correctBlocks.at(currentBlock->getIdentifier())->correctNumberOfBackLinks;
                if(!backLinksAreCorrect) {
                    NES_ERROR("\nBlock -" << currentBlock->getIdentifier() << "- contained -" << currentBlock->getNumLoopBackEdges() 
                    << "- backLinks instead of: -" << correctBlocks.at(currentBlock->getIdentifier())->correctNumberOfBackLinks << "-.");
                }
                auto correctMergeBlockId = correctBlocks.at(currentBlock->getIdentifier())->correctMergeBlockId;
                if(!correctMergeBlockId.empty()) {
                    mergeBlocksAreCorrect = ifOp->getMergeBlock()->getIdentifier() == correctMergeBlockId;
                } else {
                    mergeBlocksAreCorrect = !ifOp->getMergeBlock();
                }
                if(!mergeBlocksAreCorrect) {
                    NES_ERROR("\nMerge-Block mismatch for block " << currentBlock->getIdentifier() << ": " <<
                    ifOp->getMergeBlock()->getIdentifier() << " instead of " <<
                    correctBlocks.at(currentBlock->getIdentifier())->correctMergeBlockId << "(correct).");
                }
            }
            candidates.pop_back();
            auto terminatorOp = currentBlock->getTerminatorOp();
            if(terminatorOp->getOperationType() == IR::Operations::Operation::BranchOp) {   
                auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
                if(!visitedBlocks.contains(branchOp->getNextBlockInvocation().getBlock()->getIdentifier())) {
                    candidates.emplace_back(branchOp->getNextBlockInvocation().getBlock());
                }
            } else if (terminatorOp->getOperationType() == IR::Operations::Operation::IfOp) {
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
                if(!visitedBlocks.contains(ifOp->getFalseBlockInvocation().getBlock()->getIdentifier())) {
                    candidates.emplace_back(ifOp->getFalseBlockInvocation().getBlock());
                }
                if(!visitedBlocks.contains(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier())) {
                    candidates.emplace_back(ifOp->getTrueBlockInvocation().getBlock());
                }
            }
        } while(mergeBlocksAreCorrect && backLinksAreCorrect && !candidates.empty());
        return mergeBlocksAreCorrect;
    }
};

//==----------------------------------------------------------==//
//==------------------ NAUTILUS PASS TESTS -------------------==//
//==----------------------------------------------------------==//

void simplePrint(int printNumber) {
    printf("print number: %d", printNumber);
}

Value<> simpleLoopInfoTest() {
    Value agg = Value(0);
    Value<Int32> printNumber = Value<Int32>(42);
    Value limit = Value(100);
    for(Value i = 0; i < limit; i = i + 1) {
        FunctionCall<>("simplePrint", simplePrint, printNumber);
    }
    return agg;
}
TEST_P(StructuredControlFlowPassTest, simpleLoopInfoTets) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    // createCorrectBlock(correctBlocks, "0", 0, "7");
    // createCorrectBlock(correctBlocks, "3", 0, "9");
    // createCorrectBlock(correctBlocks, "7", 0, "8");
    auto ir = createTraceAndApplyPasses(&simpleLoopInfoTest, /* findSimpleCountedLoop */ true);
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

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
TEST_P(StructuredControlFlowPassTest, DISABLED_1_threeIfOperationsOneNestedThreeMergeBlocks) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "7");
    createCorrectBlock(correctBlocks, "3", 0, "9");
    createCorrectBlock(correctBlocks, "7", 0, "8");
    auto ir = createTraceAndApplyPasses(&threeIfOperationsOneNestedThreeMergeBlocks_1);
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
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
TEST_P(StructuredControlFlowPassTest, DISABLED_2_doubleVerticalDiamondInTrueBranch) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "9");
    createCorrectBlock(correctBlocks, "1", 0, "8");
    createCorrectBlock(correctBlocks, "8", 0, "9");
    auto ir = createTraceAndApplyPasses(&doubleVerticalDiamondInTrueBranch_2);
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
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
TEST_P(StructuredControlFlowPassTest, DISABLED_3_doubleHorizontalDiamondWithOneMergeBlockThatAlsoIsIfBlock) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "12");
    createCorrectBlock(correctBlocks, "1", 0, "12");
    createCorrectBlock(correctBlocks, "2", 0, "12");
    createCorrectBlock(correctBlocks, "12", 0, "11");
    auto ir = createTraceAndApplyPasses(&doubleHorizontalDiamondWithOneMergeBlockThatAlsoIsIfBlock_3);
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
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
TEST_P(StructuredControlFlowPassTest, DISABLED_4_oneMergeBlockThatClosesOneIfAndBecomesMergeForTwo) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "9");
    createCorrectBlock(correctBlocks, "1", 0, "9");
    createCorrectBlock(correctBlocks, "4", 0, "9");
    auto ir = createTraceAndApplyPasses(&oneMergeBlockThatClosesOneIfAndBecomesMergeForTwo_4);
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
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
TEST_P(StructuredControlFlowPassTest, DISABLED_5_oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsFollowedUpByLoopHeader) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "12");
    createCorrectBlock(correctBlocks, "1", 0, "12");
    createCorrectBlock(correctBlocks, "4", 0, "12");
    createCorrectBlock(correctBlocks, "7", 1, "");
    auto ir = createTraceAndApplyPasses(&oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsFollowedUpByLoopHeader_5);
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
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
TEST_P(StructuredControlFlowPassTest, DISABLED_6_oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsLoopHeader) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "12");
    createCorrectBlock(correctBlocks, "1", 0, "12");
    createCorrectBlock(correctBlocks, "4", 0, "12");
    createCorrectBlock(correctBlocks, "12", 1, "");
    auto ir = createTraceAndApplyPasses(&oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsLoopHeader_6);
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

Value<> loopMergeBlockBeforeCorrespondingIfOperation_7() {
    Value agg = Value(0);
    Value limit = Value(1000);
    while (agg < limit) {
        if(agg < 350) {
            agg = agg + 1;
        } else {
            agg = agg + 3;
        }
    }
    return agg;
}
TEST_P(StructuredControlFlowPassTest, DISABLED_7_loopMergeBlockBeforeCorrespondingIfOperation) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "6", 2, "");
    createCorrectBlock(correctBlocks, "1", 0, "6");
    auto ir = createTraceAndApplyPasses(&loopMergeBlockBeforeCorrespondingIfOperation_7);
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

Value<> mergeLoopMergeBlockWithLoopFollowUp_8() {
    Value agg = Value(0);
    Value limit = Value(1000000);
    if(agg < 350) {
        agg = agg + 1;
    } else {
        agg = agg + 3;
    }
    while (agg < limit) {
        if(agg < 350) {
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
TEST_P(StructuredControlFlowPassTest, DISABLED_8_mergeLoopMergeBlockWithLoopFollowUp) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "12");
    createCorrectBlock(correctBlocks, "3", 0, "12");
    createCorrectBlock(correctBlocks, "12", 2, "");
    createCorrectBlock(correctBlocks, "10", 1, "");
    auto ir = createTraceAndApplyPasses(&mergeLoopMergeBlockWithLoopFollowUp_8);
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

Value<> LoopHeaderWithNineBackLinks_9() {
    Value agg = Value(0);
    Value limit = Value(1000);
    limit.getValue().staticCast<Int64>().getValue();
    while (agg < limit) {
        if(agg < 350) {
            if(agg < 350) {
                if(agg < 350) {
                    agg = agg + 1;
                } else {
                    agg = agg + 3;
                }
                if(agg < 350) {

                } else {
                    if(agg < 350) {
                        agg = agg + 1;
                    } else {
                        agg = agg + 3;
                    }
                    agg = agg + 4;
                }
            } else {

            }
        } else {
            if(agg < 350) {
                if(agg < 350) {
                    if(agg < 350) {
                        agg = agg + 1;
                    } else {
                        agg = agg + 1;
                    }
                } else {

                }
            } else {
                if(agg < 350) {
                
                } else {
                    if(agg < 350) {
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
TEST_P(StructuredControlFlowPassTest, DISABLED_9_loopHeaderWithNineBackLinks) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "36", 9, "");
    createCorrectBlock(correctBlocks, "1", 0, "36");
    createCorrectBlock(correctBlocks, "3", 0, "36");
    createCorrectBlock(correctBlocks, "4", 0, "36");
    createCorrectBlock(correctBlocks, "5", 0, "23");
    createCorrectBlock(correctBlocks, "10", 0, "27");
    createCorrectBlock(correctBlocks, "14", 1, "");
    createCorrectBlock(correctBlocks, "16", 0, "36");
    createCorrectBlock(correctBlocks, "17", 0, "36");
    createCorrectBlock(correctBlocks, "15", 0, "36");
    createCorrectBlock(correctBlocks, "29", 0, "36");
    auto ir = createTraceAndApplyPasses(&LoopHeaderWithNineBackLinks_9);
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

Value<> mergeLoopBlock_10() {
    Value agg = Value(0);
    Value limit = Value(10);
    if(agg < 350) {
        agg = agg + 1;
    } else {
        agg = agg + 3;
    }
    while (agg < limit) {
        agg = agg + 4;
    }
    return agg;
}
TEST_P(StructuredControlFlowPassTest, DISABLED_10_mergeLoopBlock) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "6");
    createCorrectBlock(correctBlocks, "6", 1, "");
    auto ir = createTraceAndApplyPasses(&mergeLoopBlock_10);
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

Value<> IfOperationFollowedByLoopWithDeeplyNestedIfOperationsWithSeveralNestedLoops_11() {
    Value agg = Value(0);
    Value limit = Value(1000000);
    if(agg < 150) {
            agg = agg + 1;
    } else {
        if(agg < 150) {
            agg = agg + 1;
        } else {
            agg = agg + 1;
        }
    }
    for (Value start = 0; start < 10; start = start + 1) {
        if (agg < 50) {
            while (agg < limit) {
                agg = agg + 1;
            }
        } else {
            for (Value start = 0; start < 10; start = start + 1) {
                agg = agg + 1;
            }
        }
        if(agg < 150) {

        } else {//11
            if(agg < 250) {
                while (agg < limit) {
                    if(agg < 350) {
                        agg = agg + 1;
                    }
                }
                for (Value start = 0; start < 10; start = start + 1) {
                    agg = agg + 1;
                }
            }
            if(agg < 450) {
                agg = agg + 1;
            } else {
                agg = agg + 2;
            }
            if(agg < 550) {
                agg = agg + 1;
            } else {
                while (agg < limit) {
                    if(agg < 350) {
                        agg = agg + 1;
                    }
                }
            }
        }
    }
    return agg;
}
TEST_P(StructuredControlFlowPassTest, DISABLED_11_IfOperationFollowedByLoopWithDeeplyNestedIfOperationsWithSeveralNestedLoops) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    createCorrectBlock(correctBlocks, "0", 0, "43");
    createCorrectBlock(correctBlocks, "2", 0, "43");
    createCorrectBlock(correctBlocks, "12", 1, "");
    createCorrectBlock(correctBlocks, "3", 0, "19");
    createCorrectBlock(correctBlocks, "9", 1, "");
    createCorrectBlock(correctBlocks, "18", 1, "");
    createCorrectBlock(correctBlocks, "19", 0, "42"); //42 is merge block for many if-operations
    createCorrectBlock(correctBlocks, "11", 0, "35");
    createCorrectBlock(correctBlocks, "44", 2, "");
    createCorrectBlock(correctBlocks, "22", 0, "44");
    createCorrectBlock(correctBlocks, "29", 1, "");
    createCorrectBlock(correctBlocks, "35", 0, "36");
    createCorrectBlock(correctBlocks, "36", 0, "42");
    createCorrectBlock(correctBlocks, "45", 2, "");
    createCorrectBlock(correctBlocks, "37", 0, "45");
    auto ir = createTraceAndApplyPasses(&IfOperationFollowedByLoopWithDeeplyNestedIfOperationsWithSeveralNestedLoops_11);
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

Value<> emptyIfElse_12() {
    Value agg = Value(0);
    if(agg < 350) {

    } else {

    }
    return agg;
}
TEST_P(StructuredControlFlowPassTest, DISABLED_12_emptyIfElse) {
    auto ir = createTraceAndApplyPasses(&emptyIfElse_12);
    auto convertedIfOperation = ir->getRootOperation()->getFunctionBasicBlock()->getTerminatorOp();
    ASSERT_EQ(convertedIfOperation->getOperationType(), IR::Operations::Operation::BranchOp);
    auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(ir->getRootOperation()->getFunctionBasicBlock()->getTerminatorOp());
    ASSERT_EQ(branchOp->getNextBlockInvocation().getBlock()->getIdentifier(), "3");
}

Value<> MergeBlockRightAfterBranchSwitch_13() {
    Value agg = Value(0);
    if(agg < 150) {
        agg = agg + 1;
    } else {

    }
    return agg;
}
TEST_P(StructuredControlFlowPassTest, DISABLED_13_MergeBlockRightAfterBranchSwitch) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto ir = createTraceAndApplyPasses(&MergeBlockRightAfterBranchSwitch_13);
    createCorrectBlock(correctBlocks, "0", 0, "3");
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

Value<> StartBlockIsMergeBlock_14() {
    Value agg = Value(0);
    while(agg < 10) {
        if(agg < 150) {
            agg = agg + 1;
        } else {

        }
    }
    return agg;
}
TEST_P(StructuredControlFlowPassTest, DISABLED_14_StartBlockIsMergeBlock) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto ir = createTraceAndApplyPasses(&StartBlockIsMergeBlock_14);
    createCorrectBlock(correctBlocks, "6", 2, "");
    createCorrectBlock(correctBlocks, "1", 0, "6");
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

Value<> correctMergeBlockForwardingAfterFindingMergeBlocksOne_15() {
    Value agg = Value(0);
    if(agg < 150) {
        agg = agg + 1;
    } else {
        if(agg < 150) {
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
    if(agg < 150) {
        agg = agg + 1;
    } else {
        if(agg < 150) {
            agg = agg + 2;
        } else {
            agg = agg + 3;
        }
    }
    return agg;
}
TEST_P(StructuredControlFlowPassTest, DISABLED_15_correctMergeBlockForwardingAfterFindingMergeBlocks) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto ir = createTraceAndApplyPasses(&correctMergeBlockForwardingAfterFindingMergeBlocksOne_15);
    createCorrectBlock(correctBlocks, "0", 0, "5");
    createCorrectBlock(correctBlocks, "2", 0, "6");
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
    correctBlocks.clear();
    ir = createTraceAndApplyPasses(&correctMergeBlockForwardingAfterFindingMergeBlocksTwo_15);
    createCorrectBlock(correctBlocks, "0", 0, "6");
    createCorrectBlock(correctBlocks, "2", 0, "6");
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
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
TEST_P(StructuredControlFlowPassTest, DISABLED_16_OneMergeBlockThreeIfOperationsFalseBranchIntoTrueBranchIntoFalseBranch) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto ir = createTraceAndApplyPasses(&OneMergeBlockThreeIfOperationsFalseBranchIntoTrueBranchIntoFalseBranch_16);
    createCorrectBlock(correctBlocks, "0", 0, "9");
    createCorrectBlock(correctBlocks, "2", 0, "9");
    createCorrectBlock(correctBlocks, "3", 0, "9");
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}



//==----------------------------------------------------------------==//
//==------------------- TRACING/Printing BREAKERS ------------------==//
//==----------------------------------------------------------------==//
// Todo breaks tracing (empty block created), addressed by #3021
Value<> InterruptedMergeBlockForwarding_17() {
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
TEST_P(StructuredControlFlowPassTest, DISABLED_DISABLED_17_InterruptedMergeBlockForwarding) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto ir = createTraceAndApplyPasses(&InterruptedMergeBlockForwarding_17);
    // createCorrectBlock(correctBlocks, "0", 0, "11");
    // createCorrectBlock(correctBlocks, "2", 0, "11");
    // createCorrectBlock(correctBlocks, "3", 0, "11");
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

// Todo breaks tracing (empty block created), addressed by #3021
// -> leads to block without operations
Value<> TracingBreaker_18() {
    Value agg = Value(0);
    Value limit = Value(10);
    if(agg < 350) {
         agg = agg + 1;
    }
    if(agg < 350) {
        agg = agg + 1;
    } else {
        if(agg < 350) {
            if(agg < 350) { //the 'false' case of this if this if-operation has no operations -> Block_9
                agg = agg + 1;
            } else {
                agg = agg + 2; // leads to empty block
            }
        }
    }
    return agg;
}
TEST_P(StructuredControlFlowPassTest, DISABLED_DISABLED_18_TracingBreaker) {
    std::unordered_map<std::string, CorrectBlockValuesPtr> correctBlocks;
    auto ir = createTraceAndApplyPasses(&TracingBreaker_18);
    createCorrectBlock(correctBlocks, "0", 0, "5");
    createCorrectBlock(correctBlocks, "5", 0, "11");
    createCorrectBlock(correctBlocks, "4", 0, "11");
    createCorrectBlock(correctBlocks, "6", 0, "11");
    ASSERT_EQ(checkIRForCorrectness(ir->getRootOperation()->getFunctionBasicBlock(), correctBlocks), true);
}

// Tests all registered compilation backends.
// To select a specific compilation backend use ::testing::Values("MLIR") instead of ValuesIn.
auto pluginNames = Backends::CompilationBackendRegistry::getPluginNames();
INSTANTIATE_TEST_CASE_P(testLoopCompilation,
                        StructuredControlFlowPassTest,
                        ::testing::ValuesIn(pluginNames.begin(), pluginNames.end()),
                        [](const testing::TestParamInfo<StructuredControlFlowPassTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus