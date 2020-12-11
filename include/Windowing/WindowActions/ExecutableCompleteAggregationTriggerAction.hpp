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
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <memory>

namespace NES::Windowing {

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class ExecutableCompleteAggregationTriggerAction
    : public BaseExecutableWindowAction<KeyType, InputType, PartialAggregateType, FinalAggregateType> {
  public:
    static ExecutableCompleteAggregationTriggerActionPtr<KeyType, InputType, PartialAggregateType, FinalAggregateType>
    create(LogicalWindowDefinitionPtr windowDefinition,
           std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>>
               executableWindowAggregation,
           SchemaPtr outputSchema) {
        return std::make_shared<ExecutableCompleteAggregationTriggerAction<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(windowDefinition, executableWindowAggregation,
                                                                            outputSchema);
    }
    explicit ExecutableCompleteAggregationTriggerAction(
        LogicalWindowDefinitionPtr windowDefinition,
        std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>>
            executableWindowAggregation,
        SchemaPtr outputSchema)
        : windowDefinition(windowDefinition), executableWindowAggregation(executableWindowAggregation) {

        NES_DEBUG("ExecutableCompleteAggregationTriggerAction intialized with schema:" << outputSchema->toString());
        this->windowSchema = outputSchema;
        windowTupleLayout = NodeEngine::createRowLayout(this->windowSchema);
    }

    virtual ~ExecutableCompleteAggregationTriggerAction() {
        // nop
    }

    bool doAction(StateVariable<KeyType, WindowSliceStore<PartialAggregateType>*>* windowStateVariable, uint64_t currentWatermark,
                  uint64_t lastWatermark) {
        NES_DEBUG("ExecutableCompleteAggregationTriggerAction (" << this->windowDefinition->getDistributionType()->toString()
                                                                 << "): doAction for currentWatermark=" << currentWatermark
                                                                 << " lastWatermark=" << lastWatermark);

        // get the reference to the shared ptr.
        if (this->weakExecutionContext.expired()) {
            NES_FATAL_ERROR("ExecutableCompleteAggregationTriggerAction: the weakExecutionContext was already expired!");
            return false;
        }
        auto executionContext = this->weakExecutionContext.lock();
        auto tupleBuffer = executionContext->allocateTupleBuffer();

        // iterate over all keys in the window state
        for (auto& it : windowStateVariable->rangeAll()) {
            // write all window aggregates to the tuple buffer
            aggregateWindows(it.first, it.second, this->windowDefinition, tupleBuffer, currentWatermark,
                             lastWatermark);//put key into this
            NES_DEBUG("ExecutableCompleteAggregationTriggerAction (" << this->windowDefinition->getDistributionType()->toString()
                                                                     << "): " << toString() << " check key=" << it.first
                                                                     << "nextEdge=" << it.second->nextEdge);
        }

        if (tupleBuffer.getNumberOfTuples() != 0) {
            //write remaining buffer
            NES_DEBUG("ExecutableCompleteAggregationTriggerAction ("
                      << this->windowDefinition->getDistributionType()->toString()
                      << "): Dispatch last buffer output buffer with " << tupleBuffer.getNumberOfTuples()
                      << " records, content=" << UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, this->windowSchema)
                      << " originId=" << tupleBuffer.getOriginId() << "windowAction=" << toString()
                      << " currentWatermark=" << currentWatermark << " lastWatermark=" << lastWatermark);
            //forward buffer to next  pipeline stage
            executionContext->dispatchBuffer(tupleBuffer);
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
                          NodeEngine::TupleBuffer& tupleBuffer, uint64_t currentWatermark, uint64_t lastWatermark) {

        // For event time we use the maximal records ts as watermark.
        // For processing time we use the current wall clock as watermark.
        // create result vector of windows
        auto windows = std::vector<WindowState>();

        if (this->weakExecutionContext.expired()) {
            NES_FATAL_ERROR("ExecutableCompleteAggregationTriggerAction: the weakExecutionContext was already expired!");
        }
        auto executionContext = this->weakExecutionContext.lock();

        //TODO this will be replaced by the the watermark operator
        // iterate over all slices and update the partial final aggregates
        auto slices = store->getSliceMetadata();
        auto partialAggregates = store->getPartialAggregates();

        //trigger a central window operator
        for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
            NES_TRACE("ExecutableCompleteAggregationTriggerAction ("
                      << this->windowDefinition->getDistributionType()->toString() << "): trigger sliceid=" << sliceId
                      << " start=" << slices[sliceId].getStartTs() << " end=" << slices[sliceId].getEndTs());
        }

        if (currentWatermark > lastWatermark) {
            NES_DEBUG("aggregateWindows trigger because currentWatermark=" << currentWatermark
                                                                           << " > lastWatermark=" << lastWatermark);
            windowDefinition->getWindowType()->triggerWindows(windows, lastWatermark, currentWatermark);//watermark
            NES_DEBUG("ExecutableCompleteAggregationTriggerAction ("
                      << this->windowDefinition->getDistributionType()->toString()
                      << "): trigger Complete or combining window for slices=" << slices.size() << " windows=" << windows.size());
        } else {
            NES_TRACE("aggregateWindows No trigger because NOT currentWatermark=" << currentWatermark
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
                NES_TRACE("ExecutableCompleteAggregationTriggerAction ("
                          << this->windowDefinition->getDistributionType()->toString() << "): key=" << key
                          << " window.getStartTs()=" << window.getStartTs() << " slices[sliceId].getStartTs()="
                          << slices[sliceId].getStartTs() << " window.getEndTs()=" << window.getEndTs()
                          << " slices[sliceId].getEndTs()=" << slices[sliceId].getEndTs()
                          << " recCnt=" << slices[sliceId].getRecordsPerSlice());
                if (window.getStartTs() <= slices[sliceId].getStartTs() && window.getEndTs() >= slices[sliceId].getEndTs()
                    && slices[sliceId].getRecordsPerSlice() != 0) {
                    NES_TRACE("ExecutableCompleteAggregationTriggerAction ("
                              << this->windowDefinition->getDistributionType()->toString()
                              << "): create partial agg windowId=" << windowId << " sliceId=" << sliceId << " key=" << key
                              << " partAgg=" << executableWindowAggregation->lower(partialAggregates[sliceId])
                              << " recCnt=" << slices[sliceId].getRecordsPerSlice());
                    partialFinalAggregates[windowId] =
                        executableWindowAggregation->combine(partialFinalAggregates[windowId], partialAggregates[sliceId]);
                    //we have to do this in order to prevent that we output a window that has no slice associated
                    recordsPerWindow[windowId] += slices[sliceId].getRecordsPerSlice();
                } else {
                    NES_TRACE("ExecutableCompleteAggregationTriggerAction CC: condition not true");
                }
            }
        }

        uint64_t currentNumberOfTuples = tupleBuffer.getNumberOfTuples();
        if (windows.size() != 0) {
            for (uint64_t i = 0; i < partialFinalAggregates.size(); i++) {
                auto& window = windows[i];
                auto value = executableWindowAggregation->lower(partialFinalAggregates[i]);
                NES_TRACE("ExecutableCompleteAggregationTriggerAction ("
                          << this->windowDefinition->getDistributionType()->toString() << "): write i=" << i << " key=" << key
                          << " value=" << value << " window.start()=" << window.getStartTs()
                          << " window.getEndTs()=" << window.getEndTs() << " recordsPerWindow[i]=" << recordsPerWindow[i]);
                if (recordsPerWindow[i] != 0) {
                    if (windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Type::Merging) {
                        writeResultRecord<KeyType>(tupleBuffer, currentNumberOfTuples, window.getStartTs(), window.getEndTs(),
                                                   key, value, recordsPerWindow[i]);
                    } else {
                        writeResultRecord<KeyType>(tupleBuffer, currentNumberOfTuples, window.getStartTs(), window.getEndTs(),
                                                   key, value);
                    }

                    currentNumberOfTuples++;
                }

                //if we would write to a new buffer and we still have tuples to write
                if (currentNumberOfTuples * this->windowSchema->getSchemaSizeInBytes() > tupleBuffer.getBufferSize()
                    && i + 1 < partialFinalAggregates.size()) {
                    tupleBuffer.setNumberOfTuples(currentNumberOfTuples);
                    //write full buffer
                    NES_TRACE("ExecutableCompleteAggregationTriggerAction ("
                              << this->windowDefinition->getDistributionType()->toString()
                              << "): Dispatch intermediate output buffer with " << currentNumberOfTuples
                              << " records, content=" << UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, this->windowSchema)
                              << " originId=" << tupleBuffer.getOriginId() << "windowAction=" << toString() << std::endl);
                    //forward buffer to next  pipeline stage
                    executionContext->dispatchBuffer(tupleBuffer);
                    // request new buffer
                    tupleBuffer = executionContext->allocateTupleBuffer();
                    currentNumberOfTuples = 0;
                }
            }//end of for
            //erase partial aggregate and slices  as it was written
            store->removeSlicesUntil(currentWatermark);

            tupleBuffer.setNumberOfTuples(currentNumberOfTuples);
        } else {
            NES_TRACE("ExecutableCompleteAggregationTriggerAction (" << this->windowDefinition->getDistributionType()->toString()
                                                                     << "): aggregate: no window qualifies");
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
    void writeResultRecord(NodeEngine::TupleBuffer& tupleBuffer, uint64_t index, uint64_t startTs, uint64_t endTs, KeyType key,
                           ValueType value, uint64_t cnt) {
        windowTupleLayout->getValueField<uint64_t>(index, 0)->write(tupleBuffer, startTs);
        windowTupleLayout->getValueField<uint64_t>(index, 1)->write(tupleBuffer, endTs);
        windowTupleLayout->getValueField<uint64_t>(index, 2)->write(tupleBuffer, cnt);
        if (windowDefinition->isKeyed()) {
            windowTupleLayout->getValueField<KeyType>(index, 3)->write(tupleBuffer, key);
            windowTupleLayout->getValueField<ValueType>(index, 4)->write(tupleBuffer, value);
        } else {
            windowTupleLayout->getValueField<ValueType>(index, 3)->write(tupleBuffer, value);
        }
    }

    template<typename ValueType>
    void writeResultRecord(NodeEngine::TupleBuffer& tupleBuffer, uint64_t index, uint64_t startTs, uint64_t endTs, KeyType key,
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
    NodeEngine::MemoryLayoutPtr windowTupleLayout;
};
}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableCompleteAggregationTriggerAction_HPP_
