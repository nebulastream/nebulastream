/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableNestedLoopJoinTriggerAction_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableNestedLoopJoinTriggerAction_HPP_
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <State/StateManager.hpp>
#include <State/StateVariable.hpp>
#include <Util/Logger.hpp>
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
    create(LogicalJoinDefinitionPtr joinDefintion) {
        return std::make_shared<Join::ExecutableNestedLoopJoinTriggerAction<KeyType, InputTypeLeft, InputTypeRight>>(
            joinDefintion);
    }

    explicit ExecutableNestedLoopJoinTriggerAction(LogicalJoinDefinitionPtr joinDefinition) : joinDefinition(joinDefinition) {
//        windowSchema = Schema::create()
//                           ->addField(createField("start", UINT64))
//                           ->addField(createField("end", UINT64))
//                           ->addField("key", this->joinDefinition->getLeftJoinKey()->getStamp())
//                           ->addField("valueLeft", this->joinDefinition->getLeftStreamType()->getStamp())
//                           ->addField("valueRight", this->joinDefinition->getRightStreamType()->getStamp());
        windowSchema = joinDefinition->getOutputSchema();
        windowTupleLayout = NodeEngine::createRowLayout(windowSchema);
    }

    virtual ~ExecutableNestedLoopJoinTriggerAction() {
        NES_TRACE("~ExecutableNestedLoopJoinTriggerAction()");
    }

    bool doAction(StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<InputTypeLeft>*>* leftJoinState,
                  StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<InputTypeRight>*>* rightJoinSate,
                  uint64_t currentWatermarkLeft, uint64_t currentWatermarkRight, uint64_t lastWatermarkLeft,
                  uint64_t lastWatermarkRight) override {

        // get the reference to the shared ptr.
        if (this->weakExecutionContext.expired()) {
            NES_FATAL_ERROR("ExecutableCompleteAggregationTriggerAction: the weakExecutionContext was already expired!");
            return false;
        }
        auto executionContext = this->weakExecutionContext.lock();
        auto tupleBuffer = executionContext->allocateTupleBuffer();
//        tupleBuffer.setOriginId(this->originId);
        // iterate over all keys in both window states and perform the join
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction doing the nested loop join");
        for (auto& leftHashTable : leftJoinState->rangeAll()) {
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: leftHashTable" << toString() << " check key=" << leftHashTable.first
                                                                             << " nextEdge=" << leftHashTable.second->nextEdge);
            for (auto& rightHashTable : rightJoinSate->rangeAll()) {
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: rightHashTable"
                          << toString() << " check key=" << rightHashTable.first
                          << " nextEdge=" << rightHashTable.second->nextEdge);
                {
                    if (leftHashTable.first == rightHashTable.first) {

                        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: found join pair for key " << leftHashTable.first);
                        joinWindows(leftHashTable.first, leftHashTable.second, rightHashTable.second, tupleBuffer,
                                    currentWatermarkLeft, currentWatermarkRight, lastWatermarkLeft, lastWatermarkRight);
                    }
                }
            }
        }

        if (tupleBuffer.getNumberOfTuples() != 0) {
            //write remaining buffer
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: Dispatch last buffer output buffer with "
                      << tupleBuffer.getNumberOfTuples()
                      << " records, content=" << UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, windowSchema)
                      << " originId=" << tupleBuffer.getOriginId() << "windowAction=" << toString() << std::endl);
            //forward buffer to next  pipeline stage
//            this->queryManager->addWorkForNextPipeline(tupleBuffer, this->nextPipeline);
            executionContext->dispatchBuffer(tupleBuffer);
        }

        return true;
    }

    std::string toString() override { return "ExecutableNestedLoopJoinTriggerAction"; }

    uint64_t getWatermark(Windowing::WindowSliceStore<KeyType>* store) {
        auto windowType = joinDefinition->getWindowType();
        auto windowTimeType = windowType->getTimeCharacteristic();
        auto watermark = store->getMinWatermark();

        // the window type adds result windows to the windows vectors
        if (store->getLastWatermark() == 0) {
            if (windowType->isTumblingWindow()) {
                auto* tumbWindow = dynamic_cast<Windowing::TumblingWindow*>(windowType.get());
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::updateWaterMark: successful cast to TumblingWindow");
                auto initWatermark =
                    watermark < tumbWindow->getSize().getTime() ? 0 : watermark - tumbWindow->getSize().getTime();
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::getWatermark(TumblingWindow): getLastWatermark was 0 set to="
                          << initWatermark);
                store->setLastWatermark(initWatermark);
            } else if (windowType->isSlidingWindow()) {
                auto* slidWindow = dynamic_cast<Windowing::SlidingWindow*>(windowType.get());
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::updateWaterMark: successful cast to SlidingWindow");
                auto initWatermark =
                    watermark < slidWindow->getSize().getTime() ? 0 : watermark - slidWindow->getSize().getTime();
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction:: getLastWatermark was 0 set to=" << initWatermark);
                store->setLastWatermark(initWatermark);
            } else {
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::updateWaterMark: Unknown WindowType; LastWatermark was 0 and "
                          "remains 0");
            }
        } else {
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::updateWaterMark: last watermark is="
                      << store->getLastWatermark() << " for watermark=" << watermark);
        }
        return watermark;
    }

    /**
  * @brief This method iterates over all slices in the slice store and creates the final window aggregates,
  * which are written to the tuple buffer.
  * @param store
  * @param tupleBuffer
  */
    void joinWindows(KeyType key, Windowing::WindowedJoinSliceListStore<InputTypeLeft>* leftStore,
                     Windowing::WindowedJoinSliceListStore<InputTypeRight>* rightStore, NodeEngine::TupleBuffer& tupleBuffer,
                     uint64_t currentWatermarkLeft,
                     uint64_t currentWatermarkRight, uint64_t lastWatermarkLeft, uint64_t lastWatermarkRight) {
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::joinWindows:leftStore  watermark is="
                  << currentWatermarkLeft << " lastwatermark=" << lastWatermarkLeft);
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::joinWindows:rightStore  watermark is="
                  << currentWatermarkRight << " lastwatermark=" << lastWatermarkRight);

        if (this->weakExecutionContext.expired()) {
            NES_FATAL_ERROR("ExecutableCompleteAggregationTriggerAction: the weakExecutionContext was already expired!");
            return;
        }
        auto executionContext = this->weakExecutionContext.lock();
        auto windows = std::vector<Windowing::WindowState>();

        auto leftLock = std::unique_lock(leftStore->mutex());
        auto listLeft = leftStore->getAppendList();
        auto slicesLeft = leftStore->getSliceMetadata();
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction:leftStore trigger " << windows.size() << " windows, on "
                                                                             << slicesLeft.size() << " slices");
        for (uint64_t sliceId = 0; sliceId < slicesLeft.size(); sliceId++) {
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction:leftStore trigger sliceid="
                      << sliceId << " start=" << slicesLeft[sliceId].getStartTs() << " end=" << slicesLeft[sliceId].getEndTs());
        }

        auto rightLock = std::unique_lock(leftStore->mutex());
        auto slicesRight = rightStore->getSliceMetadata();
        auto listRight = rightStore->getAppendList();

        if (currentWatermarkLeft > lastWatermarkLeft) {
            NES_DEBUG("joinWindows trigger because currentWatermarkLeft=" << currentWatermarkLeft
                                                                          << " > lastWatermarkLeft=" << lastWatermarkLeft);
            joinDefinition->getWindowType()->triggerWindows(windows, lastWatermarkLeft, currentWatermarkLeft);//watermark
            NES_TRACE(
                "ExecutableNestedLoopJoinTriggerAction: trigger Complete or combining window for windows=" << windows.size());
        } else {
            NES_DEBUG("aggregateWindows No trigger because NOT currentWatermarkLeft="
                      << currentWatermarkLeft << " > lastWatermarkLeft=" << lastWatermarkLeft);
        }

        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction:leftStore trigger Complete or combining window for slices="
                  << slicesLeft.size() << " windows=" << windows.size());
        // allocate partial final aggregates for each window because we trigger each second, there could be multiple windows ready
        auto partialFinalAggregates = std::vector<KeyType>(windows.size());
        std::vector<size_t> toDelete;
        for (size_t sliceId = 0; sliceId < slicesLeft.size(); sliceId++) {
            for (size_t windowId = 0; windowId < windows.size(); windowId++) {
                auto window = windows[windowId];
                // A slice is contained in a window if the window starts before the slice and ends after the slice
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: window.getStartTs()="
                          << window.getStartTs() << " slices[sliceId].getStartTs()=" << slicesLeft[sliceId].getStartTs()
                          << " window.getEndTs()=" << window.getEndTs()
                          << " slices[sliceId].getEndTs()=" << slicesLeft[sliceId].getEndTs());
                if (window.getStartTs() <= slicesLeft[sliceId].getStartTs()
                    && window.getEndTs() >= slicesLeft[sliceId].getEndTs()) {
                    size_t currentNumberOfTuples = tupleBuffer.getNumberOfTuples();

                    if (slicesLeft[sliceId].getStartTs() == slicesRight[sliceId].getStartTs()
                        && slicesLeft[sliceId].getEndTs() == slicesRight[sliceId].getEndTs()) {
                        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: write key="
                                  << key << " window.start()=" << window.getStartTs() << " window.getEndTs()="
                                  << window.getEndTs() << " windowId=" << windowId << " sliceId=" << sliceId);
                        for (auto& left : listLeft[sliceId]) {
                            for (auto& right : listRight[sliceId]) {
//                                NES_DEBUG("join sliceId=" << sliceId << " valueLeft=" << left << " valueRight=" << right
//                                                          << " key=" << key << " sliceStart" << slicesLeft[sliceId].getStartTs()
//                                                          << " sliceEnd=" << slicesLeft[sliceId].getEndTs());
                                writeResultRecord(tupleBuffer, currentNumberOfTuples, window.getStartTs(), window.getEndTs(), key,
                                                  left, right);
                                currentNumberOfTuples++;
                                if (currentNumberOfTuples * windowSchema->getSchemaSizeInBytes() > tupleBuffer.getBufferSize()) {
                                    tupleBuffer.setNumberOfTuples(currentNumberOfTuples);
                                    executionContext->dispatchBuffer(tupleBuffer);
//                                    this->queryManager->addWorkForNextPipeline(tupleBuffer, this->nextPipeline);
                                    tupleBuffer = executionContext->allocateTupleBuffer();
//                                    tupleBuffer.setOriginId(this->originId);
                                    currentNumberOfTuples = 0;
                                }
                            }
                        }
                        tupleBuffer.setNumberOfTuples(currentNumberOfTuples);
                        toDelete.emplace_back(sliceId);
                    }
                }
            }
        }
        std::sort(toDelete.begin(), toDelete.end(), std::greater<>());
        for (auto sliceId : toDelete) {
            listLeft.erase(listLeft.begin() + sliceId);
            listRight.erase(listRight.begin() + sliceId);
            slicesLeft.erase(slicesLeft.begin() + sliceId);
            slicesRight.erase(slicesRight.begin() + sliceId);
        }
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
    void writeResultRecord(NodeEngine::TupleBuffer& tupleBuffer, uint64_t index, uint64_t startTs, uint64_t endTs, KeyType key,
                           InputTypeLeft& leftValue, InputTypeRight& rightValue) {
        windowTupleLayout->getValueField<uint64_t>(index, 0)->write(tupleBuffer, startTs);
        windowTupleLayout->getValueField<uint64_t>(index, 1)->write(tupleBuffer, endTs);
        windowTupleLayout->getValueField<KeyType>(index, 2)->write(tupleBuffer, key);
        auto sz = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(KeyType);
        memcpy(tupleBuffer.getBuffer() + index * (sz + sizeof(InputTypeLeft) + sizeof(InputTypeRight)) + sz, &leftValue, sizeof(InputTypeLeft));
        memcpy(tupleBuffer.getBuffer() + index * (sz + sizeof(InputTypeLeft) + sizeof(InputTypeRight)) + sz + sizeof(InputTypeLeft), &rightValue, sizeof(InputTypeRight));
//        windowTupleLayout->getValueField<InputTypeLeft>(index, 3)->write(tupleBuffer, leftValue);
//        windowTupleLayout->getValueField<InputTypeRight>(index, 4)->write(tupleBuffer, rightValue);
    }

    SchemaPtr getJoinSchema() override { return windowSchema; }

  private:
    LogicalJoinDefinitionPtr joinDefinition;
    SchemaPtr windowSchema;
    NodeEngine::MemoryLayoutPtr windowTupleLayout;
};
}// namespace NES::Join
#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableNestedLoopJoinTriggerAction_HPP_
