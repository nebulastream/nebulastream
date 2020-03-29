
#include <atomic>
#include <iostream>
#include <memory>
#include <Util/Logger.hpp>
#include <boost/serialization/export.hpp>
#include <State/StateManager.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <Windows/WindowHandler.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(NES::WindowHandler)
namespace NES {

WindowHandler::WindowHandler(NES::WindowDefinitionPtr windowDefinitionPtr)
    : windowDefinition(windowDefinitionPtr) {
}

bool WindowHandler::setup(QueryExecutionPlanPtr queryExecutionPlan, uint32_t pipelineStageId) {
    this->pipelineStageId = pipelineStageId;
    this->queryExecutionPlan = queryExecutionPlan;
    // Initialize WindowHandler Manager
    this->windowManager = std::make_shared<WindowManager>(this->windowDefinition);
    // Initialize StateVariable
    this->windowState = &StateManager::instance().registerState<int64_t, WindowSliceStore<int64_t>*>("window");
    return true;
}

template<class FinalAggregateType, class PartialAggregateType>
void WindowHandler::aggregateWindows(
    WindowSliceStore<PartialAggregateType>* store,
    WindowDefinitionPtr windowDefinition,
    TupleBufferPtr tupleBuffer) {

    // For event time we use the maximal records ts as watermark.
    // For processing time we use the current wall clock as watermark.
    // TODO we should add a allowed lateness to support out of order events
    auto windowTimeType = windowDefinition->windowType->getTimeCharacteristic();
    auto watermark = windowTimeType == TimeCharacteristic::ProcessingTime ? getTsFromClock() : store->getMaxTs();

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
            if (window.getStartTs() <= slices[sliceId].getStartTs() &&
                window.getEndTs() >= slices[sliceId].getEndTs()) {
                //TODO Because of this condition we currently only support SUM aggregations
                if (Sum* sumAggregation = dynamic_cast<Sum*>(windowDefinition->windowAggregation.get())) {
                    if (partialFinalAggregates.size() <= windowId) {
                        // initial the partial aggregate
                        partialFinalAggregates[windowId] = partialAggregates[sliceId];
                    } else {
                        // update the partial aggregate
                        partialFinalAggregates[windowId] =
                            sumAggregation->combine<PartialAggregateType>(partialFinalAggregates[windowId],
                                                                          partialAggregates[sliceId]);
                    }
                }
            }
        }
    }
    // calculate the final aggregate
    auto intBuffer = static_cast<FinalAggregateType*> (tupleBuffer->getBuffer());
    for (uint64_t i = 0; i < partialFinalAggregates.size(); i++) {
        //TODO Because of this condition we currently only support SUM aggregations
        if (Sum* sumAggregation = dynamic_cast<Sum*>(windowDefinition->windowAggregation.get())) {
            intBuffer[tupleBuffer->getNumberOfTuples()] =
                sumAggregation->lower<FinalAggregateType, PartialAggregateType>(partialFinalAggregates[i]);
        }
        tupleBuffer->setNumberOfTuples(tupleBuffer->getNumberOfTuples() + 1);
    }
    store->setLastWatermark(watermark);
};

void WindowHandler::trigger() {
    while(running) {
        // we currently assume processing time and only want to check for new window results every 1 second
        // todo change this when we support event time.
        sleep(1);
        NES_DEBUG("WindowHandler: check widow trigger");
        auto windowStateVariable = static_cast<StateVariable<int64_t, WindowSliceStore<int64_t>*>*>(this->windowState);
        // create the output tuple buffer
        auto tupleBuffer = BufferManager::instance().getFixSizeBuffer();
        tupleBuffer->setTupleSizeInBytes(8);
        // iterate over all keys in the window state
        for (auto& it : windowStateVariable->rangeAll()) {
            // write all window aggregates to the tuple buffer
            // TODO we currently have no handling in the case the tuple buffer is full
            this->aggregateWindows<int64_t, int64_t>(it.second, this->windowDefinition, tupleBuffer);
        }
        // if produced tuple then send the tuple buffer to the next pipeline stage or sink
        if (tupleBuffer->getNumberOfTuples() > 0) {
            NES_DEBUG("WindowHandler: Dispatch output buffer with " << tupleBuffer->getNumberOfTuples() << " records");
            Dispatcher::instance().addWorkForNextPipeline(
                tupleBuffer,
                this->queryExecutionPlan,
                this->pipelineStageId);
        }
    }
}

void* WindowHandler::getWindowState() {
    return windowState;
}

bool WindowHandler::start() {
    if (running)
        return false;
    running = true;

    NES_DEBUG("WindowHandler " << this << ": Spawn thread")
    thread = std::thread(std::bind(&WindowHandler::trigger, this));
    return true;
}

bool WindowHandler::stop() {
    NES_DEBUG("WindowHandler " << this << ": Stop called")
    if (!running)
        return false;
    running = false;

    if (thread.joinable())
        thread.join();
    NES_DEBUG("WindowHandler " << this << ": Thread joinded")
    return true;
}

WindowHandler::~WindowHandler() {NES_DEBUG("WindowHandler: calling destructor")}

const WindowHandlerPtr createWindowHandler(WindowDefinitionPtr windowDefinition) {
    return std::make_shared<WindowHandler>(windowDefinition);
}

} // namespace NES
