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

#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLESLICEAGGREGATIONTRIGGERACTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLESLICEAGGREGATIONTRIGGERACTION_HPP_
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/QueryManager.hpp>
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

namespace NES::Windowing {
template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class ExecutableSliceAggregationTriggerAction : public BaseExecutableWindowAction<KeyType, InputType, PartialAggregateType, FinalAggregateType> {
  public:
    static BaseExecutableWindowActionPtr<KeyType, InputType, PartialAggregateType, FinalAggregateType> create(LogicalWindowDefinitionPtr windowDefinition,
                                                                                                              std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> executableWindowAggregation) {
        return std::make_shared<ExecutableSliceAggregationTriggerAction>(windowDefinition, executableWindowAggregation);
    }

    ExecutableSliceAggregationTriggerAction(LogicalWindowDefinitionPtr windowDefinition,
                                            std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> executableWindowAggregation) : windowDefinition(windowDefinition),
                                                                                                                                                                             executableWindowAggregation(executableWindowAggregation) {
        windowSchema = Schema::create()
                           ->addField(createField("start", UINT64))
                           ->addField(createField("end", UINT64))
                           ->addField("key", this->windowDefinition->getOnKey()->getStamp())
                           ->addField("value", this->windowDefinition->getWindowAggregation()->as()->getStamp());
        windowTupleLayout = createRowLayout(windowSchema);
    }

    struct ResultTuple {
        ResultTuple(uint64_t start,
                    uint64_t end,
                    KeyType key,
                    PartialAggregateType value) : start(start),
                                     end(end),
                                     key(key),
                                     value(value) {
        }
        uint64_t start;
        uint64_t end;
        KeyType key;
        PartialAggregateType value;
    };

    bool doAction(StateVariable<KeyType, WindowSliceStore<PartialAggregateType>*>* windowStateVariable, QueryManagerPtr queryManager,
                  BufferManagerPtr bufferManager,
                  PipelineStagePtr nextPipeline,
                  uint64_t originId) {
        std::vector<ResultTuple> results;

        // iterate over all keys in the window state
        for (auto& it : windowStateVariable->rangeAll()) {
            NES_DEBUG("ExecutableSliceAggregationTriggerAction: " << toString() << " check key=" << it.first << "nextEdge=" << it.second->nextEdge);

            // write all window aggregates to the tuple buffer
            aggregateWindows(it.first, it.second, results);
            // TODO we currently have no handling in the case the tuple buffer is full
        }

        //write results
        if (results.size() != 0) {
            NES_DEBUG("ExecutableSliceAggregationTriggerAction: writeResultRecord write " << results.size() << " records");
            writeResults(queryManager, bufferManager, nextPipeline, originId, results);
        } else {
            NES_DEBUG("ExecutableSliceAggregationTriggerAction: writeResultRecord no records to write");
        }

        return true;
    }

    std::string toString() {
        return "ExecutableSliceAggregationTriggerAction";
    }

    uint64_t updateWaterMark(WindowSliceStore<PartialAggregateType>* store) {
        auto windowType = windowDefinition->getWindowType();
        auto windowTimeType = windowType->getTimeCharacteristic();
        auto watermark = windowTimeType->getType() == TimeCharacteristic::ProcessingTime ? getTsFromClock() : store->getMinWatermark();

        // the window type adds result windows to the windows vectors
        if (store->getLastWatermark() == 0) {
            if (windowType->isTumblingWindow()) {
                TumblingWindow* tumbWindow = dynamic_cast<TumblingWindow*>(windowType.get());
                NES_DEBUG("BaseExecutableWindowActionPtr::aggregateWindows: successful cast to TumblingWindow");
                auto initWatermark = watermark < tumbWindow->getSize().getTime() ? 0 : watermark - tumbWindow->getSize().getTime();
                NES_DEBUG("BaseExecutableWindowActionPtr::aggregateWindows(TumblingWindow): getLastWatermark was 0 set to=" << initWatermark);
                store->setLastWatermark(initWatermark);
            } else if (windowType->isSlidingWindow()) {
                SlidingWindow* slidWindow = dynamic_cast<SlidingWindow*>(windowType.get());
                NES_DEBUG("BaseExecutableWindowActionPtr::aggregateWindows: successful cast to SlidingWindow");
                auto initWatermark = watermark < slidWindow->getSize().getTime() ? 0 : watermark - slidWindow->getSize().getTime();
                NES_DEBUG("BaseExecutableWindowActionPtr::aggregateWindows(SlidingWindow): getLastWatermark was 0 set to=" << initWatermark);
                store->setLastWatermark(initWatermark);
            } else {
                NES_DEBUG("BaseExecutableWindowActionPtr::aggregateWindows: Unknown WindowType; LastWatermark was 0 and remains 0");
            }
        } else {
            NES_DEBUG("BaseExecutableWindowActionPtr::aggregateWindows: last watermark is=" << store->getLastWatermark());
        }

        return watermark;
    }

    /**
  * @brief This method iterates over all slices in the slice store and creates the final window aggregates,
  * which are written to the tuple buffer.
  * @param store
  * @param tupleBuffer
  */
    void aggregateWindows(KeyType key,
                          WindowSliceStore<PartialAggregateType>* store,
                          std::vector<ResultTuple>& results) {

        // For event time we use the maximal records ts as watermark.
        // For processing time we use the current wall clock as watermark.
        // TODO we should add a allowed lateness to support out of order events
        auto watermark = updateWaterMark(store);

        NES_DEBUG("BaseExecutableWindowActionPtr::aggregateWindows: current watermark is=" << watermark << " minWatermark=" << store->getMinWatermark());

        // create result vector of windows

        // iterate over all slices and update the partial final aggregates
        auto slices = store->getSliceMetadata();
        auto partialAggregates = store->getPartialAggregates();
        NES_DEBUG("BaseExecutableWindowActionPtr: trigger " << slices.size() << " slices");

        for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
            //test if latest tuple in window is after slice end
            if (slices[sliceId].getEndTs() <= watermark) {
                NES_DEBUG("ExecutableSliceAggregationTriggerAction write result slices[sliceId].getStartTs()=" << slices[sliceId].getStartTs() << "slices[sliceId].getEndTs()="
                                                                                                               << slices[sliceId].getEndTs() << " watermark=" << watermark << " sliceID="
                                                                                                               << sliceId);

                results.push_back(ResultTuple(slices[sliceId].getStartTs(), slices[sliceId].getEndTs() , key, partialAggregates[sliceId]));
                store->removeSlicesUntil(sliceId);
            } else {
                NES_DEBUG("ExecutableSliceAggregationTriggerAction SL: Dont write result");
            }

            store->setLastWatermark(watermark);
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
        //TODO: consider moving this to the parent class
        size_t bufferIdx = 0;
        //calculate buffer requirements
        size_t requiredBufferSpace = results.size() * windowSchema->getSchemaSizeInBytes();
        size_t numberOfRequiredBuffers = std::ceil((double) requiredBufferSpace / (double) bufferManager->getBufferSize());
        NES_DEBUG("ExecutableSliceAggregationTriggerAction: writeAggregates: allocate " << numberOfRequiredBuffers << " buffers for " << results.size() << " tuples");

        //allocate buffer
        std::vector<TupleBuffer> buffers(numberOfRequiredBuffers);
        for (size_t i = 0; i < numberOfRequiredBuffers; i++) {
            buffers[i] = bufferManager->getBufferBlocking();
            buffers[i].setOriginId(originId);
        }

        for (size_t i = 0; i < results.size(); i++) {
            writeResultRecord<PartialAggregateType>(buffers[bufferIdx],
                                                    buffers[bufferIdx].getNumberOfTuples(),
                                                    results[i].start,
                                                    results[i].end,
                                                    results[i].key,
                                                    results[i].value);

            //detect if buffer is full
            buffers[bufferIdx].setNumberOfTuples(buffers[bufferIdx].getNumberOfTuples() + 1);
            if (i * windowSchema->getSchemaSizeInBytes() > (bufferIdx + 1) * bufferManager->getBufferSize() || i == results.size() - 1) {
                NES_DEBUG("ExecutableSliceAggregationTriggerAction: Dispatch output buffer with "
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

    SchemaPtr getWindowSchema() {
        return windowSchema;
    }

  private:
    //    StateVariable<KeyType, WindowSliceStore<PartialAggregateType>*>* windowStateVariable;
    std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> executableWindowAggregation;
    LogicalWindowDefinitionPtr windowDefinition;
    SchemaPtr windowSchema;
    MemoryLayoutPtr windowTupleLayout;
};
}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLESLICEAGGREGATIONTRIGGERACTION_HPP_
