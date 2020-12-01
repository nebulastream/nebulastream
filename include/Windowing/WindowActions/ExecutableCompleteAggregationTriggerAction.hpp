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

#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableCompleteAggregationTriggerAction_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableCompleteAggregationTriggerAction_HPP_
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class ExecutableCompleteAggregationTriggerAction
    : public BaseExecutableWindowAction<KeyType, InputType, PartialAggregateType, FinalAggregateType> {
  public:
    static ExecutableCompleteAggregationTriggerActionPtr<KeyType, InputType, PartialAggregateType, FinalAggregateType>
    create(LogicalWindowDefinitionPtr windowDefinition,
           std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>>
               executableWindowAggregation) {
        return std::make_shared<ExecutableCompleteAggregationTriggerAction>(windowDefinition, executableWindowAggregation);
    }

    ExecutableCompleteAggregationTriggerAction(
        LogicalWindowDefinitionPtr windowDefinition,
        std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>>
            executableWindowAggregation)
        : windowDefinition(windowDefinition),

          executableWindowAggregation(executableWindowAggregation) {
        if (windowDefinition->isKeyed()) {
            this->windowSchema = Schema::create()
                                     ->addField(createField("start", UINT64))
                                     ->addField(createField("end", UINT64))
                                     ->addField("key", windowDefinition->getOnKey()->getStamp())
                                     ->addField("value", windowDefinition->getWindowAggregation()->as()->getStamp());
        } else {
            this->windowSchema = Schema::create()
                                     ->addField(createField("start", UINT64))
                                     ->addField(createField("end", UINT64))
                                     ->addField("value", windowDefinition->getWindowAggregation()->as()->getStamp());
        }

        windowTupleLayout = createRowLayout(this->windowSchema);
    }

    bool doAction(StateVariable<KeyType, WindowSliceStore<PartialAggregateType>*>* windowStateVariable, uint64_t currentWatermark,
                  uint64_t lastWatermark) {
        NES_DEBUG("ExecutableCompleteAggregationTriggerAction: doAction for currentWatermark="
                  << currentWatermark << " lastWatermark=" << lastWatermark);

        auto tupleBuffer = this->bufferManager->getBufferBlocking();
        tupleBuffer.setOriginId(this->originId);

        // iterate over all keys in the window state
        for (auto& it : windowStateVariable->rangeAll()) {
            NES_DEBUG("ExecutableCompleteAggregationTriggerAction: " << toString() << " check key=" << it.first
                                                                     << "nextEdge=" << it.second->nextEdge);

            // write all window aggregates to the tuple buffer
            aggregateWindows(it.first, it.second, this->windowDefinition, tupleBuffer, currentWatermark,
                             lastWatermark);//put key into this
        }

        if (tupleBuffer.getNumberOfTuples() != 0) {
            //write remaining buffer
            NES_DEBUG("ExecutableCompleteAggregationTriggerAction: Dispatch last buffer output buffer with "
                      << tupleBuffer.getNumberOfTuples()
                      << " records, content=" << UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, this->windowSchema)
                      << " originId=" << tupleBuffer.getOriginId() << "windowAction=" << toString()
                      << " currentWatermark=" << currentWatermark << " lastWatermark=" << lastWatermark);
            //forward buffer to next  pipeline stage
            this->queryManager->addWorkForNextPipeline(tupleBuffer, this->nextPipeline);
        }
        return true;
    }

    std::string toString() { return "ExecutableCompleteAggregationTriggerAction"; }
    /**
  * @brief This method iterates over all slices in the slice store and creates the final window aggregates,
  * which are written to the tuple buffer.
  * @param store
  * @param windowDefinition
  * @param tupleBuffer
  */
    void aggregateWindows(KeyType key, WindowSliceStore<PartialAggregateType>* store, LogicalWindowDefinitionPtr windowDefinition,
                          TupleBuffer& tupleBuffer, uint64_t currentWatermark, uint64_t lastWatermark) {

        // For event time we use the maximal records ts as watermark.
        // For processing time we use the current wall clock as watermark.
        // create result vector of windows
        auto windows = std::vector<WindowState>();

        //TODO this will be replaced by the the watermark operator
        // iterate over all slices and update the partial final aggregates
        auto slices = store->getSliceMetadata();
        auto partialAggregates = store->getPartialAggregates();

        //trigger a central window operator
        for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
            NES_DEBUG("ExecutableCompleteAggregationTriggerAction: trigger sliceid="
                      << sliceId << " start=" << slices[sliceId].getStartTs() << " end=" << slices[sliceId].getEndTs());
        }

        if (currentWatermark > lastWatermark) {
            NES_DEBUG("aggregateWindows trigger because currentWatermark=" << currentWatermark
                                                                           << " > lastWatermark=" << lastWatermark);
            windowDefinition->getWindowType()->triggerWindows(windows, lastWatermark, currentWatermark);//watermark
            NES_DEBUG("ExecutableCompleteAggregationTriggerAction: trigger Complete or combining window for slices="
                      << slices.size() << " windows=" << windows.size());
        } else {
            NES_DEBUG("aggregateWindows No trigger because NOT currentWatermark=" << currentWatermark
                                                                                  << " > lastWatermark=" << lastWatermark);
        }

        auto recordsPerWindow = std::vector<uint64_t>(windows.size(), 0);

        // allocate partial final aggregates for each window
        //because we trigger each second, there could be multiple windows ready
        auto partialFinalAggregates = std::vector<PartialAggregateType>(windows.size());

        for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
            for (uint64_t windowId = 0; windowId < windows.size(); windowId++) {
                auto window = windows[windowId];
                // A slice is contained in a window if the window starts before the slice and ends after the slice
                NES_DEBUG("ExecutableCompleteAggregationTriggerAction: key="
                          << key << " window.getStartTs()=" << window.getStartTs() << " slices[sliceId].getStartTs()="
                          << slices[sliceId].getStartTs() << " window.getEndTs()=" << window.getEndTs()
                          << " slices[sliceId].getEndTs()=" << slices[sliceId].getEndTs()
                          << " recCnt=" << slices[sliceId].getRecordsPerSlice());
                if (window.getStartTs() <= slices[sliceId].getStartTs() && window.getEndTs() >= slices[sliceId].getEndTs()
                    && slices[sliceId].getRecordsPerSlice() != 0) {
                    NES_DEBUG("ExecutableCompleteAggregationTriggerAction: create partial agg windowId="
                              << windowId << " sliceId=" << sliceId << " key=" << key
                              << " partAgg=" << executableWindowAggregation->lower(partialAggregates[sliceId])
                              << " recCnt=" << slices[sliceId].getRecordsPerSlice());
                    partialFinalAggregates[windowId] =
                        executableWindowAggregation->combine(partialFinalAggregates[windowId], partialAggregates[sliceId]);

                    //we have to do this in order to prevent that we output a window that has no slice associated
                    recordsPerWindow[windowId] += slices[sliceId].getRecordsPerSlice();
                } else {
                    NES_DEBUG("ExecutableCompleteAggregationTriggerAction CC: condition not true");
                }
            }
        }

        uint64_t currentNumberOfTuples = tupleBuffer.getNumberOfTuples();
        if (windows.size() != 0) {
            for (uint64_t i = 0; i < partialFinalAggregates.size(); i++) {
                auto& window = windows[i];
                auto value = executableWindowAggregation->lower(partialFinalAggregates[i]);
                NES_DEBUG("ExecutableCompleteAggregationTriggerAction: write i="
                          << i << " key=" << key << " value=" << value << " window.start()=" << window.getStartTs()
                          << " window.getEndTs()=" << window.getEndTs() << " recordsPerWindow[i]=" << recordsPerWindow[i]);
                if (recordsPerWindow[i] != 0) {
                    writeResultRecord<KeyType>(tupleBuffer, currentNumberOfTuples, window.getStartTs(), window.getEndTs(), key,
                                               value);
                    currentNumberOfTuples++;
                }

                //if we would write to a new buffer and we still have tuples to write
                if (currentNumberOfTuples * this->windowSchema->getSchemaSizeInBytes() > tupleBuffer.getBufferSize()
                    && i + 1 < partialFinalAggregates.size()) {
                    tupleBuffer.setNumberOfTuples(currentNumberOfTuples);
                    //write full buffer
                    NES_DEBUG("ExecutableCompleteAggregationTriggerAction: Dispatch intermediate output buffer with "
                              << currentNumberOfTuples
                              << " records, content=" << UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, this->windowSchema)
                              << " originId=" << tupleBuffer.getOriginId() << "windowAction=" << toString() << std::endl);
                    //forward buffer to next  pipeline stage
                    this->queryManager->addWorkForNextPipeline(tupleBuffer, this->nextPipeline);

                    // request new buffer
                    tupleBuffer = this->bufferManager->getBufferBlocking();
                    tupleBuffer.setOriginId(this->originId);
                    currentNumberOfTuples = 0;
                }
            }//end of for
            //erase partial aggregate and slices  as it was written
            //TODO: enable later
            //            store->removeSlicesUntil(partialFinalAggregates.size());

            tupleBuffer.setNumberOfTuples(currentNumberOfTuples);
        } else {
            NES_DEBUG("ExecutableCompleteAggregationTriggerAction: aggregate: no window qualifies");
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
    void writeResultRecord(TupleBuffer& tupleBuffer, uint64_t index, uint64_t startTs, uint64_t endTs, KeyType key,
                           ValueType value) {
        windowTupleLayout->getValueField<uint64_t>(index, 0)->write(tupleBuffer, startTs);
        windowTupleLayout->getValueField<uint64_t>(index, 1)->write(tupleBuffer, endTs);
        if (windowDefinition->isKeyed()) {
            windowTupleLayout->getValueField<KeyType>(index, 2)->write(tupleBuffer, key);
            windowTupleLayout->getValueField<ValueType>(index, 3)->write(tupleBuffer, value);
        } else {
            windowTupleLayout->getValueField<ValueType>(index, 2)->write(tupleBuffer, value);
        }
    }

  private:
    std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> executableWindowAggregation;
    LogicalWindowDefinitionPtr windowDefinition;
    MemoryLayoutPtr windowTupleLayout;
};
}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableCompleteAggregationTriggerAction_HPP_
