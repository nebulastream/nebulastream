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
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <State/StateManager.hpp>
#include <State/StateVariable.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/BaseExecutableJoinAction.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Join {
template<class KeyType>
class ExecutableNestedLoopJoinTriggerAction : public BaseExecutableJoinAction<KeyType> {
  public:
    static ExecutableNestedLoopJoinTriggerActionPtr<KeyType> create(LogicalJoinDefinitionPtr joinDefintion) {
        return std::make_shared<Join::ExecutableNestedLoopJoinTriggerAction<KeyType>>(joinDefintion);
    }

    ExecutableNestedLoopJoinTriggerAction(LogicalJoinDefinitionPtr joinDefinition) : joinDefinition(joinDefinition) {
        windowSchema = Schema::create()
                           ->addField(createField("start", UINT64))
                           ->addField(createField("end", UINT64))
                           ->addField("key", this->joinDefinition->getJoinKey()->getStamp())
                           ->addField("value", this->joinDefinition->getJoinKey()->getStamp());
        windowTupleLayout = createRowLayout(windowSchema);
    }

    struct ResultTuple {
        ResultTuple(uint64_t start,
                    uint64_t end,
                    KeyType key,
                    KeyType value) : start(start),
                                     end(end),
                                     key(key),
                                     value(value)
        {
        }
        uint64_t start;
        uint64_t end;
        KeyType key;
        KeyType value;
    };

    bool doAction(StateVariable<KeyType, Windowing::WindowSliceStore<KeyType>*>* leftJoinState,
                  StateVariable<KeyType, Windowing::WindowSliceStore<KeyType>*>* rightJoinSate,
                  QueryManagerPtr queryManager,
                  BufferManagerPtr bufferManager,
                  PipelineStagePtr nextPipeline,
                  uint64_t originId) {
#ifdef EXTENDEDDEBUGGING
        //Print the state content for debugging purposes
        NES_TRACE("leftJoinState content=");
        for (auto& [key, val] : leftJoinState->rangeAll()) {
            NES_TRACE("Key: " << key << " Value: " << val);
            for (auto& slice : val->getSliceMetadata()) {
                NES_TRACE("start=" << slice.getStartTs() << " end=" << slice.getEndTs());
            }
            for (auto agg : val->getPartialAggregates()) {
                NES_TRACE("key=" << key);
                NES_TRACE("value=" << agg);
            }
            NES_TRACE(" last watermark=" << val->getLastWatermark() << " minwatermark=" << val->getMinWatermark() << " allMaxTs=" << val->getAllMaxTs());
        }

        NES_TRACE("rightJoinSate content=");
        for (auto& [key, val] : rightJoinSate->rangeAll()) {
            NES_TRACE("Key: " << key << " Value: " << val);
            for (auto& slice : val->getSliceMetadata()) {
                NES_TRACE("start=" << slice.getStartTs() << " end=" << slice.getEndTs());
            }
            for (auto agg : val->getPartialAggregates()) {
                NES_TRACE("key=" << key);
                NES_TRACE("value=" << agg);
            }
            NES_TRACE(" last watermark=" << val->getLastWatermark() << " minwatermark=" << val->getMinWatermark() << " allMaxTs=" << val->getAllMaxTs());
        }
#endif

        std::vector<ResultTuple> results;
        // iterate over all keys in both window states and perform the join
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction doing the nested loop join");
        for (auto& leftHashTable : leftJoinState->rangeAll()) {
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: leftHashTable" << toString() << " check key=" << leftHashTable.first << " nextEdge=" << leftHashTable.second->nextEdge);

            for (auto& rightHashTable : rightJoinSate->rangeAll()) {
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: rightHashTable" << toString() << " check key=" << rightHashTable.first << " nextEdge=" << rightHashTable.second->nextEdge);
                {
                    if (leftHashTable.first == rightHashTable.first) {

                        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: found join pair for key " << leftHashTable.first);
                        joinWindows(leftHashTable.first, leftHashTable.second, rightHashTable.second, results);
                    }
                }
            }
        }

        //write results
        if (results.size() != 0) {
            NES_DEBUG("writeResultRecord write " << results.size() << " records");
            writeResults(queryManager, bufferManager, nextPipeline, originId, results);
        } else {
            NES_DEBUG("writeResultRecord no records to write");
        }

        //update the last seen watermark for ALL keys as the loop above only calls the joinMehtod for matching keys
        auto windowType = joinDefinition->getWindowType();
        auto windowTimeType = windowType->getTimeCharacteristic();
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction doing the nested loop join");
        for (auto& leftHashTable : leftJoinState->rangeAll()) {
            auto watermark = windowTimeType->getType() == TimeCharacteristic::ProcessingTime ? getTsFromClock() : leftHashTable.second->getMinWatermark();
            leftHashTable.second->setLastWatermark(watermark);
        }
        for (auto& rightHashTable : rightJoinSate->rangeAll()) {
            auto watermark = windowTimeType->getType() == TimeCharacteristic::ProcessingTime ? getTsFromClock() : rightHashTable.second->getMinWatermark();
            rightHashTable.second->setLastWatermark(watermark);
        }

        return true;
    }

    std::string toString() {
        return "ExecutableNestedLoopJoinTriggerAction";
    }

    uint64_t getWatermark(WindowSliceStore<KeyType>* store) {
        auto windowType = joinDefinition->getWindowType();
        auto windowTimeType = windowType->getTimeCharacteristic();
        auto watermark = windowTimeType->getType() == TimeCharacteristic::ProcessingTime ? getTsFromClock() : store->getMinWatermark();

        // the window type adds result windows to the windows vectors
        if (store->getLastWatermark() == 0) {
            if (windowType->isTumblingWindow()) {
                TumblingWindow* tumbWindow = dynamic_cast<TumblingWindow*>(windowType.get());
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::updateWaterMark: successful cast to TumblingWindow");
                auto initWatermark = watermark < tumbWindow->getSize().getTime() ? 0 : watermark - tumbWindow->getSize().getTime();
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::getWatermark(TumblingWindow): getLastWatermark was 0 set to=" << initWatermark);
                store->setLastWatermark(initWatermark);
            } else if (windowType->isSlidingWindow()) {
                SlidingWindow* slidWindow = dynamic_cast<SlidingWindow*>(windowType.get());
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::updateWaterMark: successful cast to SlidingWindow");
                auto initWatermark = watermark < slidWindow->getSize().getTime() ? 0 : watermark - slidWindow->getSize().getTime();
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::updateWaterMark(SlidingWindow): getLastWatermark was 0 set to=" << initWatermark);
                store->setLastWatermark(initWatermark);
            } else {
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::updateWaterMark: Unknown WindowType; LastWatermark was 0 and remains 0");
            }
        } else {
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::updateWaterMark: last watermark is=" << store->getLastWatermark() << " for watermark=" << watermark);
        }
        return watermark;
    }

    /**
  * @brief This method iterates over all slices in the slice store and creates the final window aggregates,
  * which are written to the tuple buffer.
  * @param store
  * @param tupleBuffer
  */
    void joinWindows(KeyType key,
                     WindowSliceStore<KeyType>* leftStore,
                     WindowSliceStore<KeyType>* rightStore,
                     std::vector<ResultTuple>& results) {

        // TODO we should add a allowed lateness to support out of order events
        auto watermarkLeft = getWatermark(leftStore);
        auto watermarkRight = getWatermark(rightStore);

        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::joinWindows:leftStore  watermark is=" << watermarkLeft << " minWatermark=" << leftStore->getMinWatermark() << " lastwatermark=" << leftStore->getLastWatermark());
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::joinWindows:rightStore  watermark is=" << watermarkRight << " minWatermark=" << rightStore->getMinWatermark() << " lastwatermark=" << rightStore->getLastWatermark());

        // create result vector of windows, note that both sides will have the same windows
        auto windows = std::vector<WindowState>();

        //do the partial aggregation on the left side
        auto slicesLeft = leftStore->getSliceMetadata();
        auto partialAggregatesLeft = leftStore->getPartialAggregates();
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction:leftStore trigger " << windows.size() << " windows, on " << slicesLeft.size() << " slices");
        for (uint64_t sliceId = 0; sliceId < slicesLeft.size(); sliceId++) {
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction:leftStore trigger sliceid=" << sliceId << " start=" << slicesLeft[sliceId].getStartTs() << " end=" << slicesLeft[sliceId].getEndTs());
        }

        auto slicesRight = rightStore->getSliceMetadata();
        auto partialAggregatesRight = rightStore->getPartialAggregates();

        joinDefinition->getWindowType()->triggerWindows(windows, leftStore->getLastWatermark(), watermarkLeft);//watermark

        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction:leftStore trigger Complete or combining window for slices=" << slicesLeft.size() << " windows=" << windows.size());
        // allocate partial final aggregates for each window because we trigger each second, there could be multiple windows ready
        auto partialFinalAggregates = std::vector<KeyType>(windows.size());
        for (uint64_t sliceId = 0; sliceId < slicesLeft.size(); sliceId++) {
            for (uint64_t windowId = 0; windowId < windows.size(); windowId++) {
                auto window = windows[windowId];
                // A slice is contained in a window if the window starts before the slice and ends after the slice
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: window.getStartTs()=" << window.getStartTs() << " slices[sliceId].getStartTs()=" << slicesLeft[sliceId].getStartTs()
                                                                                        << " window.getEndTs()=" << window.getEndTs() << " slices[sliceId].getEndTs()=" << slicesLeft[sliceId].getEndTs());
                if (window.getStartTs() <= slicesLeft[sliceId].getStartTs() && window.getEndTs() >= slicesLeft[sliceId].getEndTs()) {
                    NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: create partial agg windowId=" << windowId << " sliceId=" << sliceId);
                    partialFinalAggregates[windowId] = partialFinalAggregates[windowId] + partialAggregatesLeft[sliceId] + partialAggregatesRight[sliceId];
                } else {
                    NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: condition not true");
                }
            }
        }

        if (windows.size() != 0) {
            for (uint64_t i = 0; i < partialFinalAggregates.size(); i++) {
                auto window = windows[i];
                auto value = partialFinalAggregates[i];
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: write key=" << key
                                                                              << " value=" << value << " window.start()="
                                                                              << window.getStartTs() << " window.getEndTs()="
                                                                              << window.getEndTs());
                results.push_back(ResultTuple(window.getStartTs(), window.getEndTs(), key, value));
            }
        } else {
            NES_DEBUG("joinWindows: no window qualifies");
        }
    }

    /**
     * @brief This method writes the content of the window in n buffers
     * @param key
     * @param partialFinalAggregates
     * @param windows
     * @param queryManager
     * @param bufferManager
     * @param nextPipeline
     * @param originId
     */
    void writeResults(QueryManagerPtr queryManager,
                      BufferManagerPtr bufferManager,
                      PipelineStagePtr nextPipeline,
                      uint64_t originId,
                      std::vector<ResultTuple>& results) {
        size_t bufferIdx = 0;

        //calculate buffer requirements
        size_t requiredBufferSpace = results.size() * windowSchema->getSchemaSizeInBytes();
        size_t numberOfRequiredBuffers = std::ceil((double) requiredBufferSpace / (double) bufferManager->getBufferSize());
        NES_DEBUG("writeAggregates: allocate " << numberOfRequiredBuffers << " buffers for " << results.size() << " tuples");

        //allocate buffer
        std::vector<TupleBuffer> buffers(numberOfRequiredBuffers);
        for (size_t i = 0; i < numberOfRequiredBuffers; i++) {
            buffers[i] = bufferManager->getBufferBlocking();
            buffers[i].setOriginId(originId);
        }

        for (size_t i = 0; i < results.size(); i++) {

            writeResultRecord<KeyType>(buffers[bufferIdx],
                                       buffers[bufferIdx].getNumberOfTuples(),
                                       results[i].start,
                                       results[i].end,
                                       results[i].key,
                                       results[i].value);

            //detect if buffer is full
            buffers[bufferIdx].setNumberOfTuples(buffers[bufferIdx].getNumberOfTuples() + 1);
            //if new buffer has to be created or
            if (i * windowSchema->getSchemaSizeInBytes() > (bufferIdx + 1) * bufferManager->getBufferSize()
                || i == results.size() - 1) {
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: Dispatch output buffer with "
                          << " i * windowSchema->getSchemaSizeInBytes()=" << i * windowSchema->getSchemaSizeInBytes() << " i=" << i << " idx=" << bufferIdx
                          << " windowSchema->getSchemaSizeInBytes()=" << windowSchema->getSchemaSizeInBytes()
                          << " bufferManager->getBufferSize()=" << bufferManager->getBufferSize()
                          << " results.size()=" << results.size()
                          << buffers[bufferIdx].getNumberOfTuples() << " records, content="
                          << UtilityFunctions::prettyPrintTupleBuffer(buffers[bufferIdx], windowSchema)
                          << " originId=" << buffers[bufferIdx].getOriginId() << "windowAction=" << toString()
                          << std::endl);
                //forward buffer to next pipeline stage
                queryManager->addWorkForNextPipeline(buffers[bufferIdx], nextPipeline);
                bufferIdx++;
            }
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
    template<typename ValueType>
    void writeResultRecord(TupleBuffer& tupleBuffer, uint64_t index, uint64_t startTs, uint64_t endTs, KeyType key, ValueType value) {
        windowTupleLayout->getValueField<uint64_t>(index, 0)->write(tupleBuffer, startTs);
        windowTupleLayout->getValueField<uint64_t>(index, 1)->write(tupleBuffer, endTs);
        windowTupleLayout->getValueField<KeyType>(index, 2)->write(tupleBuffer, key);
        windowTupleLayout->getValueField<ValueType>(index, 3)->write(tupleBuffer, value);
    }

    std::string getActionResultAsString(TupleBuffer& buffer) {
        return UtilityFunctions::prettyPrintTupleBuffer(buffer, windowSchema);
    }

    SchemaPtr getJoinSchema() {
        return windowSchema;
    }

  private:
    LogicalJoinDefinitionPtr joinDefinition;
    SchemaPtr windowSchema;
    MemoryLayoutPtr windowTupleLayout;
};
}// namespace NES::Join
#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableNestedLoopJoinTriggerAction_HPP_
