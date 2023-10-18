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

#include <API/Schema.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayoutTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/WorkerContext.hpp>
#include <State/StateManager.hpp>
#include <State/StateVariable.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Operators/LogicalOperators/Windows/DistributionCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDefinition.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowState.hpp>
#include <Windowing/Runtime/WindowedJoinSliceListStore.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/BaseExecutableJoinAction.hpp>
#include <Operators/LogicalOperators/Windows/Types/SlidingWindow.hpp>
#include <Operators/LogicalOperators/Windows/Types/TumblingWindow.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowType.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <cstdint>
#include <cstring>

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
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction {} join output schema= {}", id, windowSchema->toString());
    }

    virtual ~ExecutableNestedLoopJoinTriggerAction() { NES_DEBUG("~ExecutableNestedLoopJoinTriggerAction {}:()", id); }

    bool doAction(Runtime::StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<InputTypeLeft>*>* leftJoinState,
                  Runtime::StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<InputTypeRight>*>* rightJoinSate,
                  uint64_t currentWatermark,
                  uint64_t lastWatermark,
                  Runtime::WorkerContextRef workerContext) override {

        // get the reference to the shared ptr.
        if (this->weakExecutionContext.expired()) {
            NES_FATAL_ERROR("ExecutableNestedLoopJoinTriggerAction {}:: the weakExecutionContext was already expired!", id);
            return false;
        }

        auto executionContext = this->weakExecutionContext.lock();
        auto tupleBuffer = workerContext.allocateTupleBuffer();
        windowTupleLayout = NES::Runtime::MemoryLayouts::RowLayout::create(this->windowSchema, tupleBuffer.getBufferSize());
        // iterate over all keys in both window states and perform the join
        NES_TRACE("ExecutableNestedLoopJoinTriggerAction {}:: doing the nested loop join", id);
        size_t numberOfFlushedRecords = 0;
        for (auto& leftHashTable : leftJoinState->rangeAll()) {
            NES_TRACE("ExecutableNestedLoopJoinTriggerAction {}:: leftHashTable {} check key={} nextEdge={}",
                      id,
                      toString(),
                      leftHashTable.first,
                      leftHashTable.second->nextEdge);
            for (auto& rightHashTable : rightJoinSate->rangeAll()) {
                NES_TRACE("ExecutableNestedLoopJoinTriggerAction {}:: rightHashTable {} check key={} nextEdge={}",
                          id,
                          toString(),
                          rightHashTable.first,
                          rightHashTable.second->nextEdge);
                {
                    if (joinDefinition->getJoinType() == LogicalJoinDefinition::JoinType::INNER_JOIN) {
                        if (leftHashTable.first == rightHashTable.first) {

                            NES_TRACE("ExecutableNestedLoopJoinTriggerAction {}:: found join pair for key {}",
                                      id,
                                      leftHashTable.first);
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

                        NES_TRACE("ExecutableNestedLoopJoinTriggerAction executes Cartesian Product");

                        numberOfFlushedRecords += joinWindows(leftHashTable.first,
                                                              leftHashTable.second,
                                                              rightHashTable.second,
                                                              tupleBuffer,
                                                              currentWatermark,
                                                              lastWatermark,
                                                              workerContext);
                    } else {
                        NES_ERROR("ExecutableNestedLoopJoinTriggerAction: Unknown JoinType {}",
                                  magic_enum::enum_name(joinDefinition->getJoinType()));
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
                NES_TRACE("ExecutableNestedLoopJoinTriggerAction {}:: Dispatch last buffer output buffer with {}, content={} "
                          "originId={} watermark={} windowAction={}",
                          id,
                          tupleBuffer.getNumberOfTuples(),
                          dynamicTupleBuffer,
                          tupleBuffer.getOriginId(),
                          tupleBuffer.getWatermark(),
                          toString());
            }

            //forward buffer to next  pipeline stage
            this->emitBuffer(tupleBuffer);
        }
        NES_TRACE("Join handler {} flushed {} records", toString(), numberOfFlushedRecords);
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
        NES_TRACE("ExecutableNestedLoopJoinTriggerAction {}:::joinWindows:leftStore currentWatermark is={} lastWatermark={}",
                  id,
                  currentWatermark,
                  lastWatermark);
        size_t numberOfFlushedRecords = 0;
        if (this->weakExecutionContext.expired()) {
            NES_FATAL_ERROR("ExecutableNestedLoopJoinTriggerAction {}:: the weakExecutionContext was already expired!", id);
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
            NES_WARNING("Found left store empty: {} and right store empty: {}", leftStore->empty(), rightStore->empty());
            NES_WARNING("Skipping join as left or right slices should not be empty");
            return 0;
        }

        NES_TRACE("content left side for key={}", key);
        size_t id = 0;
        for (auto& left : slicesLeft) {
            NES_TRACE("left start={} left end={} id={}", left.getStartTs(), left.getEndTs(), id++);
        }

        NES_TRACE("ExecutableNestedLoopJoinTriggerAction {}:: leftStore trigger {} windows, on {} slices",
                  id,
                  windows.size(),
                  slicesLeft.size());
        for (uint64_t sliceId = 0; sliceId < slicesLeft.size(); sliceId++) {
            NES_TRACE("ExecutableNestedLoopJoinTriggerAction {}::leftStore trigger sliceid={} start={} end={}",
                      id,
                      sliceId,
                      slicesLeft[sliceId].getStartTs(),
                      slicesLeft[sliceId].getEndTs());
        }

        NES_TRACE("ExecutableNestedLoopJoinTriggerAction {}:: content right side for key={}", id, key);
        id = 0;
        for (auto& right : slicesRight) {
            NES_TRACE("right start={} right end={} id={}", right.getStartTs(), right.getEndTs(), id++);
        }

        if (currentWatermark > lastWatermark) {
            NES_TRACE(
                "ExecutableNestedLoopJoinTriggerAction {}:: joinWindows trigger because currentWatermark={} > lastWatermark={}",
                id,
                currentWatermark,
                lastWatermark);
            Windowing::WindowType::asTimeBasedWindowType(joinDefinition->getWindowType())
                ->triggerWindows(windows, lastWatermark, currentWatermark);
        } else {
            NES_TRACE("ExecutableNestedLoopJoinTriggerAction {}:: aggregateWindows No trigger because NOT currentWatermark={} > "
                      "lastWatermark={}",
                      id,
                      currentWatermark,
                      lastWatermark);
        }

        NES_TRACE(
            "ExecutableNestedLoopJoinTriggerAction {}:: leftStore trigger Complete or combining window for slices={} windows={}",
            id,
            slicesLeft.size(),
            windows.size());
        int64_t largestClosedWindow = 0;

        for (size_t sliceId = 0; sliceId < slicesLeft.size(); sliceId++) {
            for (size_t windowId = 0; windowId < windows.size(); windowId++) {
                auto window = windows[windowId];
                largestClosedWindow = std::max((int64_t) window.getEndTs(), largestClosedWindow);

                // A slice is contained in a window if the window starts before the slice and ends after the slice
                NES_TRACE("ExecutableNestedLoopJoinTriggerAction {}:: window.getStartTs()={} slices[sliceId].getStartTs()={} "
                          "window.getEndTs()={} slices[sliceId].getEndTs()={}",
                          id,
                          window.getStartTs(),
                          slicesLeft[sliceId].getStartTs(),
                          window.getEndTs(),
                          slicesLeft[sliceId].getEndTs());
                if (window.getStartTs() <= slicesLeft[sliceId].getStartTs()
                    && window.getEndTs() >= slicesLeft[sliceId].getEndTs()) {
                    size_t currentNumberOfTuples = tupleBuffer.getNumberOfTuples();
                    NES_TRACE("ExecutableNestedLoopJoinTriggerAction {}:: window.getStartTs()={} "
                              "slices[sliceId].getStartTs()={} window.getEndTs()={} slices[sliceId].getEndTs()={}",
                              id,
                              window.getStartTs(),
                              slicesRight[sliceId].getStartTs(),
                              window.getEndTs(),
                              slicesRight[sliceId].getEndTs());
                    if (slicesLeft[sliceId].getStartTs() == slicesRight[sliceId].getStartTs()
                        && slicesLeft[sliceId].getEndTs() == slicesRight[sliceId].getEndTs()) {
                        NES_TRACE("size left={} size right={}", listLeft[sliceId].size(), listRight[sliceId].size());
                        for (auto& left : listLeft[sliceId]) {
                            for (auto& right : listRight[sliceId]) {
                                NES_TRACE("ExecutableNestedLoopJoinTriggerAction {}:: write key={} window.start()={} "
                                          "window.getEndTs()={} windowId={} sliceId={}",
                                          id,
                                          key,
                                          window.getStartTs(),
                                          window.getEndTs(),
                                          windowId,
                                          sliceId);

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
                NES_TRACE("ExecutableNestedLoopJoinTriggerAction {}:: largestClosedWindow={}  slideSize={}",
                          id,
                          largestClosedWindow,
                          slideSize);

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
        NES_TRACE("write sizes left={} right={} typeL={} typeR={}",
                  sizeof(leftValue),
                  sizeof(rightValue),
                  sizeof(InputTypeLeft),
                  sizeof(InputTypeRight));
        NES_ASSERT(windowTupleLayout->getSchema()->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT,
                   "Layout Type must be ROW");
        // A struct can be packed which assures that the correct bytes are written to the TupleBuffer.
        // (Using a std::tuple here let to inconsistencies.)
        struct __attribute__((packed)) NewRecord {
            uint64_t startTs;
            uint64_t endTs;
            KeyType key;
            InputTypeLeft leftValue;
            InputTypeRight rightValue;
        };
        auto newRecordData = NewRecord{startTs, endTs, key, leftValue, rightValue};
        // We use memcpy to write to the TupleBuffer, because (Row-/Column-)LayoutTupleBuffer were removed in #3467.
        // We cannot use a DynamicTupleBuffer leftValue, because its 'write()' function does not support structs and
        // leftValue and rightValue can be structs.
        std::memcpy(tupleBuffer.getBuffer<NewRecord>() + index, &newRecordData, sizeof(NewRecord));
        if (index + 1 > tupleBuffer.getNumberOfTuples()) {
            tupleBuffer.setNumberOfTuples(index + 1);
        }
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
