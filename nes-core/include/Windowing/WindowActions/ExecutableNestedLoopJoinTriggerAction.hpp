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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLENESTEDLOOPJOINTRIGGERACTION_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLENESTEDLOOPJOINTRIGGERACTION_HPP_
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayoutTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/WorkerContext.hpp>
#include <State/StateManager.hpp>
#include <State/StateVariable.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/Runtime/WindowedJoinSliceListStore.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/BaseExecutableJoinAction.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Join {
template<class KeyType, class InputTypeLeft, class InputTypeRight>
class ExecutableNestedLoopJoinTriggerAction : public BaseExecutableJoinAction<KeyType, InputTypeLeft, InputTypeRight> {
  public:
    static ExecutableNestedLoopJoinTriggerActionPtr<KeyType, InputTypeLeft, InputTypeRight>
    create(LogicalJoinDefinitionPtr joinDefintion, uint64_t id) {
        return std::make_shared<Join::ExecutableNestedLoopJoinTriggerAction<KeyType, InputTypeLeft, InputTypeRight>>(
            joinDefintion,
            id);
    }

    explicit ExecutableNestedLoopJoinTriggerAction(LogicalJoinDefinitionPtr joinDefinition, uint64_t id)
        : joinDefinition(joinDefinition), id(id) {
        windowSchema = joinDefinition->getOutputSchema();
        NES_DEBUG2("ExecutableNestedLoopJoinTriggerAction {} join output schema= {}", id, windowSchema->toString());
    }

    virtual ~ExecutableNestedLoopJoinTriggerAction() { NES_DEBUG2("~ExecutableNestedLoopJoinTriggerAction {}}:()", id); }

    bool doAction(Runtime::StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<InputTypeLeft>*>* leftJoinState,
                  Runtime::StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<InputTypeRight>*>* rightJoinSate,
                  uint64_t currentWatermark,
                  uint64_t lastWatermark,
                  Runtime::WorkerContextRef workerContext) override {

        // get the reference to the shared ptr.
        if (this->weakExecutionContext.expired()) {
            NES_FATAL_ERROR2("ExecutableNestedLoopJoinTriggerAction {}:: the weakExecutionContext was already expired!", id);
            return false;
        }

        auto executionContext = this->weakExecutionContext.lock();
        auto tupleBuffer = workerContext.allocateTupleBuffer();
        windowTupleLayout = NES::Runtime::MemoryLayouts::RowLayout::create(this->windowSchema, tupleBuffer.getBufferSize());
        // iterate over all keys in both window states and perform the join
        NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:: doing the nested loop join", id);
        size_t numberOfFlushedRecords = 0;
        for (auto& leftHashTable : leftJoinState->rangeAll()) {
            NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:: leftHashTable {} check key={} nextEdge={}",
                      id, toString(), leftHashTable.first, leftHashTable.second->nextEdge);
            for (auto& rightHashTable : rightJoinSate->rangeAll()) {
                NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:: rightHashTable {} check key={} nextEdge={}",
                          id, toString(), rightHashTable.first, rightHashTable.second->nextEdge);
                {
                    if (joinDefinition->getJoinType() == LogicalJoinDefinition::JoinType::INNER_JOIN) {
                        if (leftHashTable.first == rightHashTable.first) {

                            NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:: found join pair for key {}", id, leftHashTable.first);
                            numberOfFlushedRecords += joinWindows(leftHashTable.first,
                                                                  leftHashTable.second,
                                                                  rightHashTable.second,
                                                                  tupleBuffer,
                                                                  currentWatermark,
                                                                  lastWatermark,
                                                                  workerContext);
                        }
                    }

                    else if (joinDefinition->getJoinType() == LogicalJoinDefinition::JoinType::CARTESIAN_PRODUCT) {

                        NES_TRACE2("ExecutableNestedLoopJoinTriggerAction executes Cartesian Product");

                        numberOfFlushedRecords += joinWindows(leftHashTable.first,
                                                              leftHashTable.second,
                                                              rightHashTable.second,
                                                              tupleBuffer,
                                                              currentWatermark,
                                                              lastWatermark,
                                                              workerContext);
                    } else {
                        NES_ERROR2("ExecutableNestedLoopJoinTriggerAction: Unknown JoinType {}", joinDefinition->getJoinType());
                    }
                }
            }
        }

        if (tupleBuffer.getNumberOfTuples() != 0) {
            //write remaining buffer
            tupleBuffer.setOriginId(this->originId);
            tupleBuffer.setWatermark(currentWatermark);

            if (Logger::getInstance()->getCurrentLogLevel() == LogLevel::LOG_TRACE) {
                auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(windowSchema, tupleBuffer.getBufferSize());
                auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayout, tupleBuffer);
                NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:: Dispatch last buffer output buffer with {}, content={} originId={} watermark={} windowAction={}",
                           id, tupleBuffer.getNumberOfTuples(), dynamicTupleBuffer, tupleBuffer.getOriginId(), tupleBuffer.getWatermark(), toString());
            }

            //forward buffer to next  pipeline stage
            this->emitBuffer(tupleBuffer);
        }
        NES_TRACE2("Join handler {} flushed {} records", toString(), numberOfFlushedRecords);
        return true;
    }

    std::string toString() override {
        std::stringstream ss;
        ss << "ExecutableNestedLoopJoinTriggerAction " << id;
        return ss.str();
    }

    /**
     * @brief Execute the joining of all possible slices and join pairs for a given key
     * @param key the target key of the join
     * @param leftStore left content of the state
     * @param rightStore right content of the state
     * @param tupleBuffer the output buffer
     * @param currentWatermark current watermark on the left side and right side
     * @param lastWatermark last watermark on the left side and right side
     */
    size_t joinWindows(KeyType key,
                       Windowing::WindowedJoinSliceListStore<InputTypeLeft>* leftStore,
                       Windowing::WindowedJoinSliceListStore<InputTypeRight>* rightStore,
                       Runtime::TupleBuffer& tupleBuffer,
                       uint64_t currentWatermark,
                       uint64_t lastWatermark,
                       Runtime::WorkerContextRef workerContext) {
        NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:::joinWindows:leftStore currentWatermark is={} lastWatermark={}", id, currentWatermark, lastWatermark);
        size_t numberOfFlushedRecords = 0;
        if (this->weakExecutionContext.expired()) {
            NES_FATAL_ERROR2("ExecutableNestedLoopJoinTriggerAction {}:: the weakExecutionContext was already expired!", id);
            return 0;
        }
        auto executionContext = this->weakExecutionContext.lock();
        auto windows = std::vector<Windowing::WindowState>();

        auto leftLock = std::unique_lock(leftStore->mutex());
        auto listLeft = leftStore->getAppendList();
        auto slicesLeft = leftStore->getSliceMetadata();

        auto rightLock = std::unique_lock(leftStore->mutex());
        auto slicesRight = rightStore->getSliceMetadata();
        auto listRight = rightStore->getAppendList();

        if (leftStore->empty() || rightStore->empty()) {
            NES_WARNING2("Found left store empty: {} and right store empty: {}", leftStore->empty(), rightStore->empty());
            NES_WARNING2("Skipping join as left or right slices should not be empty");
            return 0;
        }

        NES_TRACE2("content left side for key={}", key);
        size_t id = 0;
        for (auto& left : slicesLeft) {
            NES_TRACE2("left start={} left end={} id={}", left.getStartTs(), left.getEndTs(), id++);
        }

        NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:: leftStore trigger {} windows, on {} slices", id, windows.size(), slicesLeft.size());
        for (uint64_t sliceId = 0; sliceId < slicesLeft.size(); sliceId++) {
            NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}::leftStore trigger sliceid={} start={} end={}", id, sliceId, slicesLeft[sliceId].getStartTs(), slicesLeft[sliceId].getEndTs());
        }

        NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:: content right side for key={}", id, key);
        id = 0;
        for (auto& right : slicesRight) {
            NES_TRACE2("right start={} right end={} id={}" , << right.getStartTs(), right.getEndTs(), id++);
        }

        if (currentWatermark > lastWatermark) {
            NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:: joinWindows trigger because currentWatermark={} > lastWatermark={}", id, currentWatermark, lastWatermark);
            Windowing::WindowType::asTimeBasedWindowType(joinDefinition->getWindowType())
                ->triggerWindows(windows, lastWatermark, currentWatermark);
        } else {
            NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:: aggregateWindows No trigger because NOT currentWatermark={} > lastWatermark={}", id, currentWatermark, lastWatermark);
        }

        NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:: leftStore trigger Complete or combining window for slices={} windows={}" , id, slicesLeft.size(), windows.size());
        int64_t largestClosedWindow = 0;

        for (size_t sliceId = 0; sliceId < slicesLeft.size(); sliceId++) {
            for (size_t windowId = 0; windowId < windows.size(); windowId++) {
                auto window = windows[windowId];
                largestClosedWindow = std::max((int64_t) window.getEndTs(), largestClosedWindow);

                // A slice is contained in a window if the window starts before the slice and ends after the slice
                NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:: window.getStartTs()={} slices[sliceId].getStartTs()={} window.getEndTs()={} slices[sliceId].getEndTs()={}",
                          id, window.getStartTs(), slicesLeft[sliceId].getStartTs(), window.getEndTs(), slicesLeft[sliceId].getEndTs());
                if (window.getStartTs() <= slicesLeft[sliceId].getStartTs()
                    && window.getEndTs() >= slicesLeft[sliceId].getEndTs()) {
                    size_t currentNumberOfTuples = tupleBuffer.getNumberOfTuples();
                    NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:: window.getStartTs()={} slices[sliceId].getStartTs()={} window.getEndTs()={} slices[sliceId].getEndTs()={}",
                               id, window.getStartTs(), slicesRight[sliceId].getStartTs(), window.getEndTs(), slicesRight[sliceId].getEndTs());
                    if (slicesLeft[sliceId].getStartTs() == slicesRight[sliceId].getStartTs()
                        && slicesLeft[sliceId].getEndTs() == slicesRight[sliceId].getEndTs()) {
                        NES_TRACE2("size left={} size right={}", listLeft[sliceId].size(), listRight[sliceId].size());
                        for (auto& left : listLeft[sliceId]) {
                            for (auto& right : listRight[sliceId]) {
                                NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:: write key={} window.start()={} window.getEndTs()={} windowId={} sliceId={}",
                                           id, key, window.getStartTs(), window.getEndTs(), windowId, sliceId);

                                if ((currentNumberOfTuples + 1) * windowSchema->getSchemaSizeInBytes()
                                    > tupleBuffer.getBufferSize()) {
                                    tupleBuffer.setNumberOfTuples(currentNumberOfTuples);
                                    executionContext->dispatchBuffer(tupleBuffer);
                                    tupleBuffer = workerContext.allocateTupleBuffer();
                                    numberOfFlushedRecords += currentNumberOfTuples;
                                    currentNumberOfTuples = 0;
                                }
                                writeResultRecord(tupleBuffer,
                                                  currentNumberOfTuples,
                                                  window.getStartTs(),
                                                  window.getEndTs(),
                                                  key,
                                                  left,
                                                  right);
                                currentNumberOfTuples++;
                            }
                        }
                        tupleBuffer.setNumberOfTuples(currentNumberOfTuples);
                    }
                }

                uint64_t slideSize =
                    Windowing::WindowType::asTimeBasedWindowType(joinDefinition->getWindowType())->getSize().getTime();
                NES_TRACE2("ExecutableNestedLoopJoinTriggerAction {}:: largestClosedWindow={}  slideSize={}" << id, largestClosedWindow, slideSize);

                //TODO: we have to re-activate the deletion once we are sure that it is working again
                largestClosedWindow = largestClosedWindow > slideSize ? largestClosedWindow - slideSize : 0;
                if (largestClosedWindow != 0) {
                    leftStore->removeSlicesUntil(largestClosedWindow);
                    rightStore->removeSlicesUntil(largestClosedWindow);
                }
            }
        }
        return numberOfFlushedRecords;
    }

    /**
    * @brief Writes a value to the output buffer with the following schema
    * -- start_ts, end_ts, key, value --
    * @tparam ValueType Type of the particular value
    * @param tupleBuffer reference to the tuple buffer we want to write to
    * @param index record index
    * @param startTs start ts of the window/slice
    * @param endTs end ts of the window/slice
    * @param key key of the value
    * @param value value
    */
    void writeResultRecord(Runtime::TupleBuffer& tupleBuffer,
                           uint64_t index,
                           uint64_t startTs,
                           uint64_t endTs,
                           KeyType key,
                           InputTypeLeft& leftValue,
                           InputTypeRight& rightValue) {
        NES_TRACE2("write sizes left={} right={} typeL={} typeR={}",
                  sizeof(leftValue) , sizeof(rightValue), sizeof(InputTypeLeft), sizeof(InputTypeRight));

        auto bindedRowLayout = windowTupleLayout->bind(tupleBuffer);
        std::tuple<uint64_t, uint64_t, KeyType, InputTypeLeft, InputTypeRight> newTuple(startTs,
                                                                                        endTs,
                                                                                        key,
                                                                                        leftValue,
                                                                                        rightValue);
        bindedRowLayout->pushRecord<true>(newTuple, index);
    }

    SchemaPtr getJoinSchema() override { return windowSchema; }

  private:
    LogicalJoinDefinitionPtr joinDefinition;
    SchemaPtr windowSchema;
    Runtime::MemoryLayouts::RowLayoutPtr windowTupleLayout;
    uint64_t id;
};
}// namespace NES::Join
#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLENESTEDLOOPJOINTRIGGERACTION_HPP_
