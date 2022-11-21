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

#include "Nautilus/IR/Operations/IfOperation.hpp"
#include "Nautilus/IR/Operations/Operation.hpp"
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Runtime/BufferManager.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <unordered_map>

using namespace NES::Nautilus;
namespace NES::Nautilus {

class StructuredControlFlowTest : public testing::Test, public AbstractCompilationBackendTest {
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

};

// Value<> sumLoop(int upperLimit) {
//     Value agg = Value(1);
//     for (Value start = 0; start < upperLimit; start = start + 1) {
//         agg = agg + 10;
//     }
//     return agg;
// }

// TEST_P(StructuredControlFlowTest, sumLoopTestSCF) {
//     auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
//         return sumLoop(10);
//     });

//     auto engine = prepare(execution);
//     auto function = engine->getInvocableMember<int32_t (*)()>("execute");
//     ASSERT_EQ(function(), 101);
// }

// Value<> nestedSumLoop(int upperLimit) {
//     Value agg = Value(1);
//     for (Value start = 0; start < upperLimit; start = start + 1) {
//         for (Value start2 = 0; start2 < upperLimit; start2 = start2 + 1) {
//             agg = agg + 10;
//         }
//     }
//     return agg;
// }

// TEST_P(StructuredControlFlowTest, nestedLoopTest) {
//     auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
//         return nestedSumLoop(10);
//     });

//     auto engine = prepare(execution);
//     auto function = engine->getInvocableMember<int32_t (*)()>("execute");
//     ASSERT_EQ(function(), 1001);
// }

// TEST_P(StructuredControlFlowTest, sumLoopTestCF) {
//     auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
//         return sumLoop(10);
//     });

//     auto engine = prepare(execution);
//     auto function = engine->getInvocableMember<int32_t (*)()>("execute");
//     ASSERT_EQ(function(), 101);
// }

// Value<> ifSumLoop() {
//     Value agg = Value(1);
//     for (Value start = 0; start < 10; start = start + 1) {
//         if (agg < 50) {
//             agg = agg + 10;
//         }
//     }
//     return agg;
// }

// TEST_P(StructuredControlFlowTest, ifSumLoopTest) {
//     auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
//         return ifSumLoop();
//     });

//     auto engine = prepare(execution);
//     auto function = engine->getInvocableMember<int32_t (*)()>("execute");
//     ASSERT_EQ(function(), 51);
// }

// Value<> ifElseSumLoop() {
//     Value agg = Value(1);
//     for (Value<Int32> start = 0; start < 10; start = start + 1) {
//         if (agg < 50) {
//             agg = agg + 10;
//         } else {
//             agg = agg + 1;
//         }
//     }
//     return agg;
// }

// TEST_P(StructuredControlFlowTest, ifElseSumLoopTest) {
//     auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
//         return ifElseSumLoop();
//     });

//     auto engine = prepare(execution);
//     auto function = engine->getInvocableMember<int32_t (*)()>("execute");
//     ASSERT_EQ(function(), 56);
// }

// Value<> elseOnlySumLoop() {
//     Value agg = Value(1);
//     for (Value start = 0; start < 10; start = start + 1) {
//         if (agg < 50) {
//         } else {
//             agg = agg + 1;
//         }
//     }
//     return agg;
// }

// TEST_P(StructuredControlFlowTest, elseOnlySumLoopTest) {
//     auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
//         return elseOnlySumLoop();
//     });

//     auto engine = prepare(execution);
//     auto function = engine->getInvocableMember<int32_t (*)()>("execute");
//     ASSERT_EQ(function(), 1);
// }

// Value<> nestedIfSumLoop() {
//     Value agg = Value(1);
//     for (Value start = 0; start < 10; start = start + 1) {
//         if (agg < 50) {
//             if (agg < 40) {
//                 agg = agg + 10;
//             }
//         } else {
//             agg = agg + 1;
//         }
//     }
//     return agg;
// }

// TEST_P(StructuredControlFlowTest, nestedIfSumLoopTest) {
//     auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
//         return nestedIfSumLoop();
//     });
//     auto engine = prepare(execution);
//     auto function = engine->getInvocableMember<int32_t (*)()>("execute");
//     ASSERT_EQ(function(), 41);
// }

// Value<> nestedIfElseSumLoop() {
//     Value agg = Value(1);
//     for (Value start = 0; start < 10; start = start + 1) {
//         if (agg < 50) {
//             if (agg < 40) {
//                 agg = agg + 10;
//             } else {
//                 agg = agg + 100;
//             }
//         } else {
//             agg = agg + 1;
//         }
//     }
//     return agg;
// }

// TEST_P(StructuredControlFlowTest, nestedIfElseSumLoopTest) {
//     auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
//         return nestedIfElseSumLoop();
//     });
//     auto engine = prepare(execution);
//     auto function = engine->getInvocableMember<int32_t (*)()>("execute");
//     ASSERT_EQ(function(), 146);
// }

Value<> oneMergeBlockThatClosesOneIfAndBecomesMergeForTwo() {
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

Value<> oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsLoopHeader() {
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
    for (Value j = 0; j < agg; j = j + 1) {
        agg = agg + 1;
    }
    return agg;
}


void inline addPredecessorToBlock(IR::BasicBlockPtr previousBlock, std::vector<IR::BasicBlockPtr>& candidates, 
                                  std::unordered_set<std::string> visitedBlocks) {
    auto terminatorOp = previousBlock->getTerminatorOp();
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
}

bool iterateOverIR(IR::BasicBlockPtr currentBlock, const std::unordered_map<std::string, std::pair<uint32_t, std::string>>& correctMergeBlocks) {
    std::vector<IR::BasicBlockPtr> candidates;
    std::unordered_set<std::string> visitedBlocks; //todo use ptr instead?
    candidates.push_back(currentBlock);
    bool mergeBlocksAreCorrect = true;
    bool backLinksAreCorrect = true;
    do {
        visitedBlocks.emplace(currentBlock->getIdentifier());
        currentBlock = candidates.back();
        if(correctMergeBlocks.contains(currentBlock->getIdentifier())) {
            auto ifOp =  std::static_pointer_cast<IR::Operations::IfOperation>(currentBlock->getTerminatorOp());
            backLinksAreCorrect = currentBlock->getBackLinks() == correctMergeBlocks.at(currentBlock->getIdentifier()).first;
            if(!backLinksAreCorrect) {
                NES_ERROR("\nBlock -" << currentBlock->getIdentifier() << "- contained -" << currentBlock->getBackLinks() 
                << "- backLinks instead of: -" << correctMergeBlocks.at(currentBlock->getIdentifier()).first << "-.");
            }
            auto correctMergeBlockId = correctMergeBlocks.at(currentBlock->getIdentifier()).second;
            if(!correctMergeBlockId.empty()) {
                mergeBlocksAreCorrect = ifOp->getMergeBlock()->getIdentifier() == correctMergeBlockId;
            } else {
                mergeBlocksAreCorrect = !ifOp->getMergeBlock();
            }
            if(!mergeBlocksAreCorrect) {
                NES_ERROR("\nBlock -" << currentBlock->getIdentifier() << "- did not contain an if-operation with Block: -" <<
                correctMergeBlocks.at(currentBlock->getIdentifier()).second << "- set as merge block.");
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

TEST_P(StructuredControlFlowTest, nestedElseOnlySumLoop) {
    std::unordered_map<std::string, std::pair<uint32_t, std::string>> correctMergeBlocks;
    correctMergeBlocks.emplace(std::pair{"0", std::pair{0, "12"}});
    correctMergeBlocks.emplace(std::pair{"1", std::pair{0, "12"}});
    correctMergeBlocks.emplace(std::pair{"4", std::pair{0, "12"}});
    correctMergeBlocks.emplace(std::pair{"7", std::pair{1, ""}});
    // correctMergeBlocks.emplace(std::pair{"0", std::pair{0, "9"}});
    // correctMergeBlocks.emplace(std::pair{"1", std::pair{0, "9"}});
    // correctMergeBlocks.emplace(std::pair{"4", std::pair{0, "9"}});
    auto execution = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return oneMergeBlockThatClosesOneIfAndBecomesMergeForTwoAndIsLoopHeader();
    });
    auto executionTrace = ssaCreationPhase.apply(std::move(execution));
    auto ir = irCreationPhase.apply(executionTrace);
    ir = removeBrOnlyBlocksPhase.apply(ir);
    ASSERT_EQ(iterateOverIR(ir->getRootOperation()->getFunctionBasicBlock(), correctMergeBlocks), true);
    // auto engine = prepare(execution);
    
    // auto function = engine->getInvocableMember<int32_t (*)()>("execute");
    // ASSERT_EQ(function(), 1);
}

// Tests all registered compilation backends.
// To select a specific compilation backend use ::testing::Values("MLIR") instead of ValuesIn.
auto pluginNames = Backends::CompilationBackendRegistry::getPluginNames();
INSTANTIATE_TEST_CASE_P(testLoopCompilation,
                        StructuredControlFlowTest,
                        ::testing::ValuesIn(pluginNames.begin(), pluginNames.end()),
                        [](const testing::TestParamInfo<StructuredControlFlowTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus