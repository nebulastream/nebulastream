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

#ifndef NES_INCLUDE_WINDOWING_WINDOW_ACTIONS_EXECUTABLE_SLICE_AGGREGATION_TRIGGER_ACTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOW_ACTIONS_EXECUTABLE_SLICE_AGGREGATION_TRIGGER_ACTION_HPP_
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <utility>

namespace NES::Windowing {
template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class ExecutableSliceAggregationTriggerAction
    : public BaseExecutableWindowAction<KeyType, InputType, PartialAggregateType, FinalAggregateType> {
  public:
    static BaseExecutableWindowActionPtr<KeyType, InputType, PartialAggregateType, FinalAggregateType>
    create(LogicalWindowDefinitionPtr windowDefinition,
           std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>>
               executableWindowAggregation,
           SchemaPtr outputSchema,
           uint64_t id) {
        return std::make_shared<ExecutableSliceAggregationTriggerAction>(windowDefinition,
                                                                         executableWindowAggregation,
                                                                         outputSchema,
                                                                         id);
    }

    virtual ~ExecutableSliceAggregationTriggerAction() {
        //nop
    }

    ExecutableSliceAggregationTriggerAction(
        LogicalWindowDefinitionPtr windowDefinition,
        std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>>
            executableWindowAggregation,
        const SchemaPtr& outputSchema,
        uint64_t id)
        : windowDefinition(std::move(windowDefinition)), executableWindowAggregation(executableWindowAggregation), id(id) {

        this->windowSchema = outputSchema;
    }

    bool doAction(Runtime::StateVariable<KeyType, WindowSliceStore<PartialAggregateType>*>* windowStateVariable,
                  uint64_t currentWatermark,
                  uint64_t lastWatermark,
                  Runtime::WorkerContextRef workerContext) {
        NES_DEBUG("ExecutableSliceAggregationTriggerAction " << id << ": doAction for currentWatermark=" << currentWatermark
                                                             << " lastWatermark=" << lastWatermark);

        if (this->weakExecutionContext.expired()) {
            NES_FATAL_ERROR("ExecutableSliceAggregationTriggerAction " << id
                                                                       << ": the weakExecutionContext was already expired!");
            return false;
        }
        auto executionContext = this->weakExecutionContext.lock();
        auto tupleBuffer = workerContext.allocateTupleBuffer();

        windowTupleLayout = Runtime::MemoryLayouts::RowLayout::create(this->windowSchema, tupleBuffer.getBufferSize());
        tupleBuffer.setOriginId(windowDefinition->getOriginId());
        // iterate over all keys in the window state
        for (auto& it : windowStateVariable->rangeAll()) {
            NES_DEBUG("ExecutableSliceAggregationTriggerAction " << id << ": " << toString() << " check key=" << it.first
                                                                 << "nextEdge=" << it.second->nextEdge);

            // write all window aggregates to the tuple buffer
            aggregateWindows(it.first, it.second, tupleBuffer, currentWatermark, lastWatermark, workerContext);
        }

        if (tupleBuffer.getNumberOfTuples() != 0) {
            //write remaining buffer
            NES_DEBUG("ExecutableSliceAggregationTriggerAction "
                      << id << ": Dispatch last buffer output buffer with " << tupleBuffer.getNumberOfTuples()
                      << " currentWatermark=" << currentWatermark << " lastWatermark=" << lastWatermark
                      << " records, content=" << Util::prettyPrintTupleBuffer(tupleBuffer, this->windowSchema)
                      << " originId=" << tupleBuffer.getOriginId() << "windowAction=" << toString()
                      << " this->nextPipeline=" << executionContext->toString());
            //forward buffer to next  pipeline stage
            this->emitBuffer(tupleBuffer);
        }
        return true;
    }

    std::string toString() { return "ExecutableSliceAggregationTriggerAction"; }

    /**
  * @brief This method iterates over all slices in the slice store and creates the final window aggregates,
  * which are written to the tuple buffer.
  * @param store
  * @param tupleBuffer
  */
    void aggregateWindows(KeyType key,
                          WindowSliceStore<PartialAggregateType>* store,
                          Runtime::TupleBuffer& tupleBuffer,
                          uint64_t currentWatermark,
                          uint64_t lastWatermark,
                          Runtime::WorkerContextRef workerContext) {

        // For event time we use the maximal records ts as watermark.
        // For processing time we use the current wall clock as watermark.
        // TODO we should add a allowed lateness to support out of order events

        if (this->weakExecutionContext.expired()) {
            NES_FATAL_ERROR("ExecutableCompleteAggregationTriggerAction: the weakExecutionContext was already expired!");
            return;
        }
        auto executionContext = this->weakExecutionContext.lock();

        // create result vector of windows
        // iterate over all slices and update the partial final aggregates
        auto slices = store->getSliceMetadata();
        auto partialAggregates = store->getPartialAggregates();
        uint64_t currentNumberOfTuples = tupleBuffer.getNumberOfTuples();
        uint64_t maxSliceEnd = 0;

        NES_DEBUG("ExecutableSliceAggregationTriggerAction "
                  << id << ": trigger " << slices.size() << " slices "
                  << " key=" << key << " current watermark is=" << currentWatermark << " lastWatermark=" << lastWatermark
                  << " currentNumberOfTuples=" << currentNumberOfTuples
                  << " tupleBuffer.getNumberOfTuples()=" << tupleBuffer.getNumberOfTuples());

        for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
            //test if latest tuple in window is after slice end
            NES_DEBUG("ExecutableSliceAggregationTriggerAction " << id << ": check slice start=" << slices[sliceId].getStartTs()
                                                                 << " end=" << slices[sliceId].getEndTs() << " key=" << key
                                                                 << " currentWatermark=" << currentWatermark);
            if (slices[sliceId].getEndTs() <= currentWatermark) {
                NES_TRACE("ExecutableSliceAggregationTriggerAction "
                          << id << ": write result slices[sliceId].getStartTs()=" << slices[sliceId].getStartTs()
                          << "slices[sliceId].getEndTs()=" << slices[sliceId].getEndTs() << " currentWatermark="
                          << currentWatermark << " sliceID=" << sliceId << " recCnt=" << slices[sliceId].getRecordsPerSlice());

                //if we would write to a new buffer and we still have tuples to write
                if ((currentNumberOfTuples + 1) * this->windowSchema->getSchemaSizeInBytes() > tupleBuffer.getBufferSize()
                    && sliceId + 1 < slices.size()) {
                    tupleBuffer.setNumberOfTuples(currentNumberOfTuples);
                    //write full buffer
                    NES_DEBUG("ExecutableSliceAggregationTriggerAction "
                              << id << ": Dispatch intermediate output buffer with " << currentNumberOfTuples
                              << " records, content=" << Util::prettyPrintTupleBuffer(tupleBuffer, this->windowSchema)
                              << " originId=" << tupleBuffer.getOriginId() << "windowAction=" << toString());

                    //forward buffer to next  pipeline stage
                    this->emitBuffer(tupleBuffer);

                    // request new buffer
                    tupleBuffer = workerContext.allocateTupleBuffer();
                    tupleBuffer.setOriginId(windowDefinition->getOriginId());
                    currentNumberOfTuples = 0;
                }
                //TODO: we only need to send slides that are not empty
                writeResultRecord<PartialAggregateType>(tupleBuffer,
                                                        currentNumberOfTuples,
                                                        slices[sliceId].getStartTs(),
                                                        slices[sliceId].getEndTs(),
                                                        key,
                                                        partialAggregates[sliceId],
                                                        slices[sliceId].getRecordsPerSlice());
                currentNumberOfTuples++;
                maxSliceEnd = std::max(maxSliceEnd, slices[sliceId].getEndTs());

            } else {
                NES_DEBUG("ExecutableSliceAggregationTriggerAction "
                          << id << ": SL: Dont write result because slices[sliceId].getEndTs()=" << slices[sliceId].getEndTs()
                          << "<= currentWatermark=" << currentWatermark);
            }
        }//end of for
         //remove the old slices from current watermark
        store->removeSlicesUntil(currentWatermark);
        tupleBuffer.setNumberOfTuples(currentNumberOfTuples);
        tupleBuffer.setWatermark(std::max(maxSliceEnd, tupleBuffer.getWatermark()));
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

    void writeResultRecord(Runtime::TupleBuffer& tupleBuffer,
                           uint64_t index,
                           uint64_t startTs,
                           uint64_t endTs,
                           KeyType key,
                           ValueType value,
                           uint64_t cnt) {
        auto bindedRowLayout = windowTupleLayout->bind(tupleBuffer);

        if (windowDefinition->isKeyed()) {
            std::tuple<uint64_t, uint64_t, uint64_t, KeyType, ValueType> keyedTuple(startTs, endTs, cnt, key, value);
            bindedRowLayout->pushRecord<true>(keyedTuple, index);
        } else {
            std::tuple<uint64_t, uint64_t, uint64_t, ValueType> notKeyedTuple(startTs, endTs, cnt, value);
            bindedRowLayout->pushRecord<true>(notKeyedTuple, index);
        }
    }

  private:
    std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> executableWindowAggregation;
    LogicalWindowDefinitionPtr windowDefinition;
    Runtime::MemoryLayouts::RowLayoutPtr windowTupleLayout;
    uint64_t id;
};
}// namespace NES::Windowing
#endif// NES_INCLUDE_WINDOWING_WINDOW_ACTIONS_EXECUTABLE_SLICE_AGGREGATION_TRIGGER_ACTION_HPP_
