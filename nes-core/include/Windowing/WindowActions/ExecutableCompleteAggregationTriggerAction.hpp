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

#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLECOMPLETEAGGREGATIONTRIGGERACTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLECOMPLETEAGGREGATIONTRIGGERACTION_HPP_
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/Runtime/SliceMetaData.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <memory>
#include <utility>

namespace NES::Windowing {

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class ExecutableCompleteAggregationTriggerAction
    : public BaseExecutableWindowAction<KeyType, InputType, PartialAggregateType, FinalAggregateType> {
  public:
    static ExecutableCompleteAggregationTriggerActionPtr<KeyType, InputType, PartialAggregateType, FinalAggregateType>
    create(LogicalWindowDefinitionPtr windowDefinition,
           std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>>
               executableWindowAggregation,
           SchemaPtr outputSchema,
           uint64_t id,
           PartialAggregateType partialAggregateTypeInitialValue) {
        return std::make_shared<
            ExecutableCompleteAggregationTriggerAction<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(
            windowDefinition,
            executableWindowAggregation,
            outputSchema,
            id,
            partialAggregateTypeInitialValue);
    }
    explicit ExecutableCompleteAggregationTriggerAction(
        LogicalWindowDefinitionPtr windowDefinition,
        std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>>
            executableWindowAggregation,
        const SchemaPtr& outputSchema,
        uint64_t id,
        PartialAggregateType partialAggregateTypeInitialValue)
        : executableWindowAggregation(std::move(executableWindowAggregation)), windowDefinition(std::move(windowDefinition)),
          id(id), partialAggregateTypeInitialValue(partialAggregateTypeInitialValue) {

        NES_DEBUG("ExecutableCompleteAggregationTriggerAction intialized with schema:" << outputSchema->toString()
                                                                                       << " id=" << id);
        this->windowSchema = outputSchema;
    }

    bool doAction(Runtime::StateVariable<KeyType, WindowSliceStore<PartialAggregateType>*>* windowStateVariable,
                  uint64_t currentWatermark,
                  uint64_t lastWatermark,
                  Runtime::WorkerContextRef workerContext) override {
        NES_DEBUG("ExecutableCompleteAggregationTriggerAction (id="
                  << id << " " << this->windowDefinition->getDistributionType()->toString()
                  << "): doAction for currentWatermark=" << currentWatermark << " lastWatermark=" << lastWatermark);

        // get the reference to the shared ptr.
        if (this->weakExecutionContext.expired()) {
            NES_FATAL_ERROR("ExecutableCompleteAggregationTriggerAction: the weakExecutionContext was already expired!");
            return false;
        }

        auto executionContext = this->weakExecutionContext.lock();
        auto tupleBuffer = workerContext.allocateTupleBuffer();

        tupleBuffer.setOriginId(windowDefinition->getOriginId());

        // iterate over all keys in the window state
        for (auto& it : windowStateVariable->rangeAll()) {
            // write all window aggregates to the tuple buffer
            aggregateWindows(it.first,
                             it.second,
                             this->windowDefinition,
                             tupleBuffer,
                             currentWatermark,
                             lastWatermark,
                             workerContext);//put key into this
            NES_DEBUG("ExecutableCompleteAggregationTriggerAction (" << this->windowDefinition->getDistributionType()->toString()
                                                                     << "): " << toString() << " check key=" << it.first
                                                                     << "nextEdge=" << it.second->nextEdge << " id=" << id);
        }

        if (tupleBuffer.getNumberOfTuples() != 0) {
            tupleBuffer.setWatermark(currentWatermark);
            tupleBuffer.setOriginId(windowDefinition->getOriginId());
            //write remaining buffer
            NES_DEBUG("ExecutableCompleteAggregationTriggerAction ("
                      << this->windowDefinition->getDistributionType()->toString()
                      << "): Dispatch last buffer output buffer with " << tupleBuffer.getNumberOfTuples()
                      << " records, content=" << Util::prettyPrintTupleBuffer(tupleBuffer, this->windowSchema)
                      << " originId=" << tupleBuffer.getOriginId() << "windowAction=" << toString()
                      << " currentWatermark=" << currentWatermark << " lastWatermark=" << lastWatermark);
            //forward buffer to next  pipeline stage
            this->emitBuffer(tupleBuffer);
        } else {
            tupleBuffer.release();
        }
        return true;
    }

    std::string toString() override { return "ExecutableCompleteAggregationTriggerAction"; }
    /**
  * @brief This method iterates over all slices in the slice store and creates the final window aggregates,
  * which are written to the tuple buffer.
  * @param store
  * @param windowDefinition
  * @param tupleBuffer
  */
    void aggregateWindows(KeyType key,
                          WindowSliceStore<PartialAggregateType>* store,
                          const LogicalWindowDefinitionPtr& windowDef,
                          Runtime::TupleBuffer& tupleBuffer,
                          uint64_t currentWatermark,
                          uint64_t lastWatermark,
                          Runtime::WorkerContextRef workerContext) {

        NES_DEBUG("AggregateWindows for ExecutableCompleteAggregationTriggerAction id=" << id);
        // For event time we use the maximal records ts as watermark.
        // For processing time we use the current wall clock as watermark.
        // create result vector of windows
        auto windows = std::vector<WindowState>();

        if (this->weakExecutionContext.expired()) {
            NES_FATAL_ERROR(
                "ExecutableCompleteAggregationTriggerAction id=" << id << ": the weakExecutionContext was already expired!");
        }
        auto executionContext = this->weakExecutionContext.lock();

        //TODO this will be replaced by the the watermark operator
        // iterate over all slices and update the partial final aggregates
        auto slices = store->getSliceMetadata();
        auto partialAggregates = store->getPartialAggregates();
        if (slices.empty()) {
            return;
        }
        uint64_t slideSize = windowDef->getWindowType()->getSize().getTime();

        //trigger a window operator
        for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
            NES_DEBUG("ExecutableCompleteAggregationTriggerAction"
                      << id << ": (" << this->windowDefinition->getDistributionType()->toString() << "): trigger sliceid="
                      << sliceId << " start=" << slices[sliceId].getStartTs() << " end=" << slices[sliceId].getEndTs());
        }

        if (currentWatermark > lastWatermark) {
            NES_TRACE("ExecutableCompleteAggregationTriggerAction "
                      << id << ": aggregateWindows trigger because currentWatermark=" << currentWatermark
                      << " > lastWatermark=" << lastWatermark);
            windowDef->getWindowType()->triggerWindows(windows, lastWatermark, currentWatermark);//watermark
            NES_TRACE("ExecutableCompleteAggregationTriggerAction "
                      << id << " (" << this->windowDefinition->getDistributionType()->toString()
                      << "): trigger Complete or combining window for slices=" << slices.size() << " windows=" << windows.size());
        } else {
            NES_DEBUG("ExecutableCompleteAggregationTriggerAction "
                      << id << ": aggregateWindows No trigger because NOT currentWatermark=" << currentWatermark
                      << " > lastWatermark=" << lastWatermark);
        }

        //we have to sort the windows as the sliding window put them out in inverse order which produces failures with initializing windows
        sort(windows.begin(), windows.end(), [](const WindowState& lhs, const WindowState& rhs) {
            return lhs.getStartTs() < rhs.getStartTs();
        });

        auto recordsPerWindow = std::vector<uint64_t>(windows.size(), 0);

        auto partialFinalAggregates = std::vector<PartialAggregateType>(windows.size());

        partialFinalAggregates = std::vector<PartialAggregateType>(windows.size(), partialAggregateTypeInitialValue);

        //because we trigger each second, there could be multiple windows ready
        for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
            for (uint64_t windowId = 0; windowId < windows.size(); windowId++) {
                auto window = windows[windowId];
                // A slice is contained in a window if the window starts before the slice and ends after the slice
                NES_DEBUG("ExecutableCompleteAggregationTriggerAction "
                          << id << ": (" << this->windowDefinition->getDistributionType()->toString() << "): key=" << key
                          << " window.getStartTs()=" << window.getStartTs() << " slices[sliceId].getStartTs()="
                          << slices[sliceId].getStartTs() << " window.getEndTs()=" << window.getEndTs()
                          << " slices[sliceId].getEndTs()=" << slices[sliceId].getEndTs()
                          << " recCnt=" << slices[sliceId].getRecordsPerSlice());
                if (window.getStartTs() <= slices[sliceId].getStartTs() && window.getEndTs() >= slices[sliceId].getEndTs()
                    && slices[sliceId].getRecordsPerSlice() != 0) {
                    NES_TRACE("ExecutableCompleteAggregationTriggerAction "
                              << id << ": (" << this->windowDefinition->getDistributionType()->toString()
                              << "): create partial agg windowId=" << windowId << " sliceId=" << sliceId << " key=" << key
                              << " partAgg=" << executableWindowAggregation->lower(partialAggregates[sliceId])
                              << " recCnt=" << slices[sliceId].getRecordsPerSlice());
                    partialFinalAggregates[windowId] =
                        executableWindowAggregation->combine(partialFinalAggregates[windowId], partialAggregates[sliceId]);
                    //we have to do this in order to prevent that we output a window that has no slice associated
                    recordsPerWindow[windowId] += slices[sliceId].getRecordsPerSlice();
                } else {
                    NES_TRACE("ExecutableCompleteAggregationTriggerAction " << id << ": CC: condition not true");
                }
            }
        }

        uint64_t currentNumberOfTuples = tupleBuffer.getNumberOfTuples();
        if (!windows.empty()) {
            int64_t largestClosedWindow = 0;
            for (auto i = static_cast<typename decltype(partialFinalAggregates)::size_type>(0); i < partialFinalAggregates.size();
                 ++i) {
                auto& window = windows[i];
                largestClosedWindow = std::max((int64_t) window.getEndTs(), largestClosedWindow);
                auto value = executableWindowAggregation->lower(partialFinalAggregates[i]);
                NES_TRACE("ExecutableCompleteAggregationTriggerAction "
                          << id << ": (" << this->windowDefinition->getDistributionType()->toString() << "): write i=" << i
                          << " key=" << key << " value=" << value << " window.start()=" << window.getStartTs()
                          << " window.getEndTs()=" << window.getEndTs() << " recordsPerWindow[i]=" << recordsPerWindow[i]);

                //if we would write to a new buffer and we still have tuples to write
                if ((currentNumberOfTuples + 1) * this->windowSchema->getSchemaSizeInBytes() > tupleBuffer.getBufferSize()) {
                    tupleBuffer.setNumberOfTuples(currentNumberOfTuples);
                    //write full buffer
                    NES_TRACE("ExecutableCompleteAggregationTriggerAction "
                              << id << ": (" << this->windowDefinition->getDistributionType()->toString()
                              << "): Dispatch intermediate output buffer with " << currentNumberOfTuples
                              << " records, content=" << Util::prettyPrintTupleBuffer(tupleBuffer, this->windowSchema)
                              << " originId=" << tupleBuffer.getOriginId() << "windowAction=" << toString());
                    //forward buffer to next  pipeline stage
                    executionContext->dispatchBuffer(tupleBuffer);

                    // request new buffer
                    tupleBuffer = workerContext.allocateTupleBuffer();
                    currentNumberOfTuples = 0;
                }

                if (recordsPerWindow[i] != 0) {
                    if (windowDef->getDistributionType()->getType() == DistributionCharacteristic::Type::Merging) {
                        writeResultRecord<FinalAggregateType>(tupleBuffer,
                                                              currentNumberOfTuples,
                                                              window.getStartTs(),
                                                              window.getEndTs(),
                                                              key,
                                                              value,
                                                              recordsPerWindow[i]);
                    } else {
                        writeResultRecord<FinalAggregateType>(tupleBuffer,
                                                              currentNumberOfTuples,
                                                              window.getStartTs(),
                                                              window.getEndTs(),
                                                              key,
                                                              value);
                    }

                    currentNumberOfTuples++;
                }
            }//end of for
            NES_TRACE("ExecutableCompleteAggregationTriggerAction " << id << ": ("
                                                                    << this->windowDefinition->getDistributionType()->toString()
                                                                    << "): remove slices until=" << currentWatermark);
            //remove the old slices from current watermark - allowed lateness as there could be no tuple before that
            if (largestClosedWindow != 0) {
                store->removeSlicesUntil(std::abs(largestClosedWindow - (int64_t) slideSize));
            }

            tupleBuffer.setNumberOfTuples(currentNumberOfTuples);
        } else {
            NES_TRACE("ExecutableCompleteAggregationTriggerAction " << id << ": ("
                                                                    << this->windowDefinition->getDistributionType()->toString()
                                                                    << "): aggregate: no window qualifies");
        }
    }

    /**
    * @brief Writes a value to the output buffer with the following schema (key can be omitted)
    * -- start_ts, end_ts, cnt, key, value --
    * @tparam ValueType Type of the particular value
    * @param tupleBuffer reference to the tuple buffer we want to write to
    * @param index record index
    * @param startTs start ts of the window/slice
    * @param endTs end ts of the window/slice
    * @param key key of the value
    * @param value value
    */
    template<typename ValueType>

    void writeResultRecord(Runtime::TupleBuffer& tupleBuffer,
                           uint64_t index,
                           uint64_t startTs,
                           uint64_t endTs,
                           KeyType key,
                           ValueType value,
                           uint64_t cnt) {
        auto windowTupleLayout = Runtime::MemoryLayouts::RowLayout::create(this->windowSchema, tupleBuffer.getBufferSize());
        std::shared_ptr<Runtime::MemoryLayouts::RowLayoutTupleBuffer> bindedRowLayout = windowTupleLayout->bind(tupleBuffer);
        if (windowDefinition->isKeyed()) {
            std::tuple<uint64_t, uint64_t, uint64_t, KeyType, ValueType> keyedTuple(startTs, endTs, cnt, key, value);
            bindedRowLayout->pushRecord<true>(keyedTuple, index);
        } else {
            std::tuple<uint64_t, uint64_t, uint64_t, ValueType> notKeyedTuple(startTs, endTs, cnt, value);
            bindedRowLayout->pushRecord<true>(notKeyedTuple, index);
        }
    }

    /**
     * @brief Writes a value to the output buffer with the following schema (key can be omitted)
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
    void writeResultRecord(Runtime::TupleBuffer& tupleBuffer,
                           uint64_t index,
                           uint64_t startTs,
                           uint64_t endTs,
                           KeyType key,
                           ValueType value) {
        auto windowTupleLayout = Runtime::MemoryLayouts::RowLayout::create(this->windowSchema, tupleBuffer.getBufferSize());
        std::shared_ptr<Runtime::MemoryLayouts::RowLayoutTupleBuffer> bindedRowLayout = windowTupleLayout->bind(tupleBuffer);
        if (windowDefinition->isKeyed()) {
            std::tuple<uint64_t, uint64_t, KeyType, ValueType> keyedTuple(startTs, endTs, key, value);
            bindedRowLayout->pushRecord<true>(keyedTuple, index);
        } else {
            std::tuple<uint64_t, uint64_t, ValueType> notKeyedTuple(startTs, endTs, value);
            bindedRowLayout->pushRecord<true>(notKeyedTuple, index);
        }
    }

  private:
    std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> executableWindowAggregation;
    LogicalWindowDefinitionPtr windowDefinition;
    uint64_t id;
    PartialAggregateType partialAggregateTypeInitialValue;
};
}// namespace NES::Windowing
#endif  // NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLECOMPLETEAGGREGATIONTRIGGERACTION_HPP_
