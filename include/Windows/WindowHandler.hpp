#ifndef INCLUDE_WINDOWS_WINDOW_HPP_
#define INCLUDE_WINDOWS_WINDOW_HPP_

#include <API/Window/WindowDefinition.hpp>
#include <QueryLib/WindowManagerLib.hpp>
#include <Util/Logger.hpp>
#include <atomic>
#include <iostream>
#include <memory>
#include <thread>

#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <boost/serialization/export.hpp>
#include <cstring>

namespace NES {
class QueryManager;
typedef std::shared_ptr<QueryManager> QueryManagerPtr;

class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class WindowHandler;
typedef std::shared_ptr<WindowHandler> WindowHandlerPtr;

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

/**
 * @brief This represents a window during query execution.
 */
class WindowHandler {
  public:
    WindowHandler() = default;
    WindowHandler(WindowDefinitionPtr windowDefinition, QueryManagerPtr queryManager, BufferManagerPtr bufferManager);

    static WindowHandlerPtr create(WindowDefinitionPtr windowDefinition, QueryManagerPtr queryManager,
                                   BufferManagerPtr bufferManager);

    ~WindowHandler();

    /**
     * @brief Initialises the state of this window depending on the window definition.
     */
    bool setup(PipelineStagePtr nextPipeline, uint32_t pipelineStageId);

    /**
     * @brief Starts thread to check if the window should be triggered.
     * @return boolean if the window thread is started
     */
    bool start();

    /**
     * @brief Stops the window thread.
     * @return
     */
    bool stop();

    /**
     * @brief triggers all ready windows.
     * @return
     */
    void trigger();

    /**
     * @brief updates all maxTs in all stores
     * @param ts
     * @param originId
     */
    void updateAllMaxTs(uint64_t ts, uint64_t originId);

    /**
     * @brief This method iterates over all slices in the slice store and creates the final window aggregates,
     * which are written to the tuple buffer.
     * @param store
     * @param windowDefinition
     * @param tupleBuffer
     */
    template<class KeyType, class FinalAggregateType, class PartialAggregateType>
    void aggregateWindows(KeyType key, WindowSliceStore<PartialAggregateType>* store, WindowDefinitionPtr windowDefinition,
                          TupleBuffer& tupleBuffer);

    void* getWindowState();
    WindowManagerPtr getWindowManager() { return windowManager; };

    uint64_t getOriginId() const {
        return originId;
    }
    void setOriginId(uint64_t originId) {
        this->originId = originId;
    }

  private:
    std::atomic_bool running{false};
    WindowDefinitionPtr windowDefinition;
    WindowManagerPtr windowManager;
    PipelineStagePtr nextPipeline;
    void* windowState;
    std::shared_ptr<std::thread> thread;
    std::mutex runningTriggerMutex;
    uint32_t pipelineStageId;
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    uint64_t originId;

    MemoryLayoutPtr windowTupleLayout;
    SchemaPtr windowTupleSchema;

    template<class KeyType, class AggregationType>
    void writeResultRecord(TupleBuffer& tupleBuffer, uint64_t index, uint64_t startTs, uint64_t endTs, KeyType key, AggregationType value);
};

template<class KeyType, class AggregationType>
void WindowHandler::writeResultRecord(TupleBuffer& tupleBuffer, uint64_t index, uint64_t startTs, uint64_t endTs, KeyType key, AggregationType value) {
    windowTupleLayout->getValueField<uint64_t>(index, 0)->write(tupleBuffer, startTs);
    windowTupleLayout->getValueField<uint64_t>(index, 1)->write(tupleBuffer, endTs);
    windowTupleLayout->getValueField<KeyType>(index, 2)->write(tupleBuffer, key);
    windowTupleLayout->getValueField<AggregationType>(index, 3)->write(tupleBuffer, value);
}

// TODO Maybe we could define template specialization of this method when generating compiled code so that we dont need casting
template<class KeyType, class FinalAggregateType, class PartialAggregateType>
void WindowHandler::aggregateWindows(KeyType key, WindowSliceStore<PartialAggregateType>* store, WindowDefinitionPtr windowDefinition,
                                     TupleBuffer& tupleBuffer) {

    // For event time we use the maximal records ts as watermark.
    // For processing time we use the current wall clock as watermark.
    // TODO we should add a allowed lateness to support out of order events
    auto windowType = windowDefinition->getWindowType();
    auto windowTimeType = windowType->getTimeCharacteristic();
    auto watermark = windowTimeType->getType() == TimeCharacteristic::ProcessingTime ? getTsFromClock() : store->getMinWatermark();
    NES_DEBUG("WindowHandler::aggregateWindows: current watermark is=" << watermark << " minWatermark=" << store->getMinWatermark());

    // create result vector of windows
    auto windows = std::make_shared<std::vector<WindowState>>();
    // the window type adds result windows to the windows vectors
    if (store->getLastWatermark() == 0) {
        if (windowType->isTumblingWindow()) {
            TumblingWindow* tumbWindow = dynamic_cast<TumblingWindow*>(windowType.get());
            NES_DEBUG("WindowHandler::aggregateWindows: successful cast to TumblingWindow");
            auto initWatermark = watermark < tumbWindow->getSize().getTime() ? 0 : watermark - tumbWindow->getSize().getTime();
            NES_DEBUG("WindowHandler::aggregateWindows(TumblingWindow): getLastWatermark was 0 set to=" << initWatermark);
            store->setLastWatermark(initWatermark);
        } else if (windowType->isSlidingWindow()) {
            SlidingWindow* slidWindow = dynamic_cast<SlidingWindow*>(windowType.get());
            NES_DEBUG("WindowHandler::aggregateWindows: successful cast to SlidingWindow");
            auto initWatermark = watermark < slidWindow->getSize().getTime() ? 0 : watermark - slidWindow->getSize().getTime();
            NES_DEBUG("WindowHandler::aggregateWindows(SlidingWindow): getLastWatermark was 0 set to=" << initWatermark);
            store->setLastWatermark(initWatermark);
        } else {
            NES_DEBUG("WindowHandler::aggregateWindows: Unkown WindowType; LastWatermark was 0 and remains 0");
        }
    } else {
        NES_DEBUG("WindowHandler::aggregateWindows: last watermark is=" << store->getLastWatermark());
    }

    // iterate over all slices and update the partial final aggregates
    auto slices = store->getSliceMetadata();
    auto partialAggregates = store->getPartialAggregates();
    NES_DEBUG("WindowHandler: trigger " << windows->size() << " windows, on " << slices.size() << " slices");

    //trigger a central window operator
    if (windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Complete || windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Combining) {
        for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
            NES_DEBUG("WindowHandler: trigger sliceid=" << sliceId << " start=" << slices[sliceId].getStartTs() << " end=" << slices[sliceId].getEndTs());
        }
        //generates a list of windows that have to be outputted

        NES_DEBUG("WindowHandler: trigger test if mappings=" << store->getNumberOfMappings() << " < inputEdges=" << windowDefinition->getNumberOfInputEdges());
        if (store->getNumberOfMappings() < windowDefinition->getNumberOfInputEdges()) {
            NES_DEBUG("WindowHandler: trigger cause only " << store->getNumberOfMappings() << " mappings we set watermark to last=" << store->getLastWatermark());
            watermark = store->getLastWatermark();
        }
        windowDefinition->getWindowType()->triggerWindows(windows, store->getLastWatermark(), watermark);//watermark
        NES_DEBUG("WindowHandler: trigger Complete or combining window for slices=" << slices.size() << " windows=" << windows->size());

        // allocate partial final aggregates for each window
        //because we trigger each second, there could be multiple windows ready
        auto partialFinalAggregates = std::vector<PartialAggregateType>(windows->size());
        for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
            for (uint64_t windowId = 0; windowId < windows->size(); windowId++) {
                auto window = (*windows)[windowId];
                // A slice is contained in a window if the window starts before the slice and ends after the slice
                NES_DEBUG("WindowHandler CC: window.getStartTs()=" << window.getStartTs() << " slices[sliceId].getStartTs()=" << slices[sliceId].getStartTs()
                                                                   << " window.getEndTs()=" << window.getEndTs() << " slices[sliceId].getEndTs()=" << slices[sliceId].getEndTs());
                if (window.getStartTs() <= slices[sliceId].getStartTs() && window.getEndTs() >= slices[sliceId].getEndTs()) {
                    NES_DEBUG("WindowHandler CC: create partial agg windowId=" << windowId << " sliceId=" << sliceId);
                    // TODO Because of this condition we currently only support SUM aggregations
                    if (Sum* sumAggregation = dynamic_cast<Sum*>(windowDefinition->getWindowAggregation().get())) {
                        if (partialFinalAggregates.size() <= windowId) {
                            // initial the partial aggregate
                            NES_DEBUG("WindowHandler CC: assign partialAggregates[sliceId]=" << partialAggregates[sliceId] << " old value was " << partialFinalAggregates[windowId]);
                            partialFinalAggregates[windowId] = partialAggregates[sliceId];
                        } else {
                            NES_DEBUG("WindowHandler CC: update partialFinalAggregates[windowId]=" << partialFinalAggregates[windowId] << " with " << sumAggregation->combine<PartialAggregateType>(partialFinalAggregates[windowId], partialAggregates[sliceId]));
                            // update the partial aggregate
                            partialFinalAggregates[windowId] = sumAggregation->combine<PartialAggregateType>(
                                partialFinalAggregates[windowId], partialAggregates[sliceId]);
                        }
                    }
                } else {
                    NES_DEBUG("WindowHandler CC: condition not true");
                }
            }
        }
        // calculate the final aggregate
        for (uint64_t i = 0; i < partialFinalAggregates.size(); i++) {
            auto window = (*windows)[i];
            FinalAggregateType value;
            if (auto sumAggregation = std::dynamic_pointer_cast<Sum>(windowDefinition->getWindowAggregation())) {
                value = sumAggregation->lower<FinalAggregateType, PartialAggregateType>(partialFinalAggregates[i]);
            } else if (auto maxAggregation = std::dynamic_pointer_cast<Max>(windowDefinition->getWindowAggregation())) {
                value = maxAggregation->lower<FinalAggregateType, PartialAggregateType>(partialAggregates[i]);
            } else if (auto minAggregation = std::dynamic_pointer_cast<Min>(windowDefinition->getWindowAggregation())) {
                value = minAggregation->lower<FinalAggregateType, PartialAggregateType>(partialAggregates[i]);
            } else {
                NES_FATAL_ERROR("Window Handler: could not cast aggregation type");
            }
            NES_DEBUG("Window Handler: write key=" << key << " value=" << value << " window.start()="
                                                   << window.getStartTs() << " window.getEndTs()="
                                                   << window.getEndTs() << " tupleBuffer.getNumberOfTuples())" << tupleBuffer.getNumberOfTuples());

            writeResultRecord<KeyType, FinalAggregateType>(tupleBuffer,
                                                           tupleBuffer.getNumberOfTuples(),
                                                           window.getStartTs(),
                                                           window.getEndTs(),
                                                           key,
                                                           value);

            //TODO: we have to determine which windwos and keys to delete
            tupleBuffer.setNumberOfTuples(tupleBuffer.getNumberOfTuples() + 1);
        }
        //TODO: remove content from state

    } else if (windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Slicing) {
        //if slice creator, find slices which can be send but did not send already
        NES_DEBUG("WindowHandler SL: trigger Slicing for " << slices.size() << " slices");

        for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
            //test if latest tuple in window is after slice end
            NES_DEBUG("WindowHandler SL:  << slices[sliceId].getStartTs()=" << slices[sliceId].getStartTs() << "slices[sliceId].getEndTs()=" << slices[sliceId].getEndTs() << " watermark=" << watermark << " sliceID=" << sliceId);
            if (slices[sliceId].getEndTs() <= watermark) {
                NES_DEBUG("WindowHandler SL: write result");
                writeResultRecord<KeyType, FinalAggregateType>(tupleBuffer,
                                                               tupleBuffer.getNumberOfTuples(),
                                                               slices[sliceId].getStartTs(),
                                                               slices[sliceId].getEndTs(),
                                                               key,
                                                               partialAggregates[sliceId]);
                tupleBuffer.setNumberOfTuples(tupleBuffer.getNumberOfTuples() + 1);
                store->cleanupToPos(sliceId);
            } else {
                NES_DEBUG("WindowHandler SL: Dont write result");
            }
        }
    } else {
        NES_ERROR("Window combiner not implemented yet");
        NES_NOT_IMPLEMENTED();
    }

    store->setLastWatermark(watermark);
}

}// namespace NES
#endif /* INCLUDE_WINDOWS_WINDOW_HPP_ */
