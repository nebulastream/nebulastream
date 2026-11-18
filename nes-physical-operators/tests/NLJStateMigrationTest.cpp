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

#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/JoinTriggerStrategy.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <ReconfigurationState.hpp>
#include <SliceCacheConfiguration.hpp>

namespace NES
{
namespace
{
struct MinimalPipelineExecutionContext final : PipelineExecutionContext
{
    explicit MinimalPipelineExecutionContext(std::shared_ptr<BufferManager> bufferManager) : bufferManager(std::move(bufferManager)) { }

    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return true; }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { }

    TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return WorkerThreadId(0); }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 2; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return handlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>&) override { }

    [[nodiscard]] ReconfigurationState& getReconfigurationState() override { return reconfigurationState; }

    std::shared_ptr<BufferManager> bufferManager;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
    ReconfigurationState reconfigurationState{LocalQueryId(LocalQueryId::INVALID)};
};

constexpr uint64_t WINDOW_SIZE = 1000;
constexpr uint64_t TUPLE_SIZE = 16;
const std::vector LEFT_AND_RIGHT{OriginId(1), OriginId(2)};
const auto OUTPUT_ORIGIN = OriginId(3);
}

class NLJStateMigrationTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("NLJStateMigrationTest.log", LogLevel::LOG_DEBUG); }

    static std::shared_ptr<NLJOperatorHandler> createHandler()
    {
        return std::make_shared<NLJOperatorHandler>(
            LEFT_AND_RIGHT,
            OUTPUT_ORIGIN,
            std::make_unique<DefaultTimeBasedSliceStore>(WINDOW_SIZE, WINDOW_SIZE, SliceCacheConfiguration(false, 0)),
            InnerJoinTriggerStrategy{});
    }

    std::shared_ptr<BufferManager> bufferManager
        = BufferManager::create(64 * 1024 * 1024, 0.5, BufferAlignment{64}, 8192, std::make_shared<NesDefaultMemoryAllocator>());
    MinimalPipelineExecutionContext pec{bufferManager};
};

/// The migrated state contains the progress of the watermark processors and the slices of windows that were not triggered yet.
TEST_F(NLJStateMigrationTest, RestoresSlicesAndProgress)
{
    const auto oldJoin = createHandler();
    oldJoin->start(pec);
    const CreateNewNLJSliceArgs sliceArgs(*bufferManager, TUPLE_SIZE, TUPLE_SIZE);
    for (const auto timestamp : {Timestamp(10), Timestamp(1500), Timestamp(2500)})
    {
        (void)oldJoin->getSliceAndWindowStore().getSlicesOrCreate(timestamp, oldJoin->getCreateNewSlicesFunction(sliceArgs));
    }
    /// Both inputs delivered sequence numbers 1 to 3; the first window is triggered.
    for (uint64_t sequenceNumber = 1; sequenceNumber <= 3; ++sequenceNumber)
    {
        for (const auto origin : LEFT_AND_RIGHT)
        {
            oldJoin->checkAndTriggerWindows(
                BufferMetaData(
                    Timestamp(1000 + sequenceNumber), SequenceData(SequenceNumber(sequenceNumber), INITIAL_CHUNK_NUMBER, true), origin),
                &pec);
        }
    }

    const auto state = oldJoin->serializeState(*bufferManager);
    ASSERT_FALSE(state.empty());

    const auto newJoin = createHandler();
    newJoin->start(pec);
    newJoin->restoreState(state, *bufferManager);
    /// The slices of the second and third window were migrated; the first window was triggered before.
    EXPECT_FALSE(newJoin->getSliceAndWindowStore().getSliceBySliceEnd(SliceEnd(1000)).has_value());
    EXPECT_TRUE(newJoin->getSliceAndWindowStore().getSliceBySliceEnd(SliceEnd(2000)).has_value());
    EXPECT_TRUE(newJoin->getSliceAndWindowStore().getSliceBySliceEnd(SliceEnd(3000)).has_value());

    /// The new join continues the sequence numbers of its inputs: sequence number 4 advances the watermark and triggers the second window.
    for (const auto origin : LEFT_AND_RIGHT)
    {
        newJoin->checkAndTriggerWindows(
            BufferMetaData(Timestamp(2001), SequenceData(SequenceNumber(4), INITIAL_CHUNK_NUMBER, true), origin), &pec);
    }
    const auto counters = newJoin->getSliceAndWindowStore().getMigratedState();
    EXPECT_GT(counters.nextSequenceNumber, oldJoin->getSliceAndWindowStore().getMigratedState().nextSequenceNumber);
}

TEST_F(NLJStateMigrationTest, RejectsOtherState)
{
    const auto join = createHandler();
    join->start(pec);
    auto buffer = bufferManager->getBufferBlocking();
    buffer.setNumberOfTuples(8);
    EXPECT_ANY_THROW(join->restoreState({buffer}, *bufferManager));
}

}
