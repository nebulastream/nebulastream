#ifndef INCLUDE_WINDOWS_WINDOW_HPP_
#define INCLUDE_WINDOWS_WINDOW_HPP_

#include <API/Window/WindowDefinition.hpp>
#include <QueryLib/WindowManagerLib.hpp>
#include <Util/Logger.hpp>
#include <atomic>
#include <iostream>
#include <memory>
#include <thread>

#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <boost/serialization/export.hpp>

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
    //    WindowHandler(WindowDefinitionPtr windowDefinition, BufferManagerPtr bufferManager, QueryManagerPtr queryManager);

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
     * @brief This method iterates over all slices in the slice store and creates the final window aggregates,
     * which are written to the tuple buffer.
     * @tparam FinalAggregateType
     * @tparam PartialAggregateType
     * @param watermark
     * @param store
     * @param windowDefinition
     * @param tupleBuffer
     */
    template<class FinalAggregateType, class PartialAggregateType>
    void aggregateWindows(WindowSliceStore<PartialAggregateType>* store, WindowDefinitionPtr windowDefinition,
                          TupleBuffer& tupleBuffer);

    void* getWindowState();
    WindowManagerPtr getWindowManager() { return windowManager; };

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
};

// TODO Maybe we could define template specialization of this method when generating compiled code so that we dont need casting
template<class FinalAggregateType, class PartialAggregateType>
void WindowHandler::aggregateWindows(WindowSliceStore<PartialAggregateType>* store, WindowDefinitionPtr windowDefinition,
                                     TupleBuffer& tupleBuffer) {

    // For event time we use the maximal records ts as watermark.
    // For processing time we use the current wall clock as watermark.
    // TODO we should add a allowed lateness to support out of order events
    auto windowTimeType = windowDefinition->windowType->getTimeCharacteristic();
    auto watermark = windowTimeType->getType() == TimeCharacteristic::ProcessingTime ? getTsFromClock() : store->getMaxTs();

    // create result vector of windows
    auto windows = std::make_shared<std::vector<WindowState>>();
    // the window type addes result windows to the windows vectors
    windowDefinition->windowType->triggerWindows(windows, store->getLastWatermark(), watermark);
    // allocate partial final aggregates for each window
    auto partialFinalAggregates = std::vector<PartialAggregateType>(windows->size());
    // iterate over all slices and update the partial final aggregates
    auto slices = store->getSliceMetadata();
    auto partialAggregates = store->getPartialAggregates();
    NES_DEBUG("WindowHandler: trigger " << windows->size() << " windows, on " << slices.size() << " slices");
    for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
        for (uint64_t windowId = 0; windowId < windows->size(); windowId++) {
            auto window = (*windows)[windowId];
            // A slice is contained in a window if the window starts before the slice and ends after the slice
            if (window.getStartTs() <= slices[sliceId].getStartTs() && window.getEndTs() >= slices[sliceId].getEndTs()) {
                // TODO Because of this condition we currently only support SUM aggregations
                if (Sum* sumAggregation = dynamic_cast<Sum*>(windowDefinition->windowAggregation.get())) {
                    if (partialFinalAggregates.size() <= windowId) {
                        // initial the partial aggregate
                        partialFinalAggregates[windowId] = partialAggregates[sliceId];
                    } else {
                        // update the partial aggregate
                        partialFinalAggregates[windowId] = sumAggregation->combine<PartialAggregateType>(
                            partialFinalAggregates[windowId], partialAggregates[sliceId]);
                    }
                }
            }
        }
    }
    // calculate the final aggregate
    auto intBuffer = tupleBuffer.getBufferAs<FinalAggregateType>();
    for (uint64_t i = 0; i < partialFinalAggregates.size(); i++) {
        // TODO Because of this condition we currently only support SUM aggregations
        if (Sum* sumAggregation = dynamic_cast<Sum*>(windowDefinition->windowAggregation.get())) {
            intBuffer[tupleBuffer.getNumberOfTuples()] =
                sumAggregation->lower<FinalAggregateType, PartialAggregateType>(partialFinalAggregates[i]);
        }
        tupleBuffer.setNumberOfTuples(tupleBuffer.getNumberOfTuples() + 1);
    }
    store->setLastWatermark(watermark);
}

// just for test compability
const WindowHandlerPtr createTestWindow(size_t campainCnt, size_t windowSizeInSec);
const WindowHandlerPtr createWindowHandler(WindowDefinitionPtr windowDefinition, QueryManagerPtr queryManager,
                                           BufferManagerPtr bufferManager);

}// namespace NES
#endif /* INCLUDE_WINDOWS_WINDOW_HPP_ */
