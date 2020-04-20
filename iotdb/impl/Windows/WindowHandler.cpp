
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
    this->thread.reset();
}

bool WindowHandler::setup(QueryExecutionPlanPtr queryExecutionPlan, uint32_t pipelineStageId) {
    this->pipelineStageId = pipelineStageId;
    this->queryExecutionPlan = queryExecutionPlan;
    this->thread.reset();
    // Initialize WindowHandler Manager
    this->windowManager = std::make_shared<WindowManager>(this->windowDefinition);
    // Initialize StateVariable
    this->windowState = &StateManager::instance().registerState<int64_t, WindowSliceStore<int64_t>*>("window");
    return true;
}

void WindowHandler::trigger() {
    while (running) {
        // we currently assume processing time and only want to check for new window results every 1 second
        // todo change this when we support event time.
        sleep(1);
        NES_DEBUG("WindowHandler: check widow trigger");
        auto windowStateVariable = static_cast<StateVariable<int64_t, WindowSliceStore<int64_t>*>*>(this->windowState);
        // create the output tuple buffer
        // TODO can we make it get the buffer only once?
        auto tupleBuffer = BufferManager::instance().getBufferBlocking();
        tupleBuffer.setTupleSizeInBytes(8);
        // iterate over all keys in the window state
        for (auto& it : windowStateVariable->rangeAll()) {
            // write all window aggregates to the tuple buffer
            // TODO we currently have no handling in the case the tuple buffer is full
            this->aggregateWindows<int64_t, int64_t>(it.second, this->windowDefinition, tupleBuffer);
        }
        // if produced tuple then send the tuple buffer to the next pipeline stage or sink
        if (tupleBuffer.getNumberOfTuples() > 0) {
            NES_DEBUG("WindowHandler: Dispatch output buffer with " << tupleBuffer.getNumberOfTuples() << " records");
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
    std::unique_lock lock(runningTriggerMutex);
    if (running) {
        return false;
    }
    running = true;
    NES_DEBUG("WindowHandler " << this << ": Spawn thread")
    thread = std::make_shared<std::thread>([this]() {
        trigger();
    });
    return true;
}

bool WindowHandler::stop() {
    std::unique_lock lock(runningTriggerMutex);
    NES_DEBUG("WindowHandler " << this << ": Stop called");
    if (!running) {
        NES_DEBUG("WindowHandler " << this << ": Stop called but was already not running");
        return false;
    }
    running = false;

    if (thread && thread->joinable()) {
        thread->join();
    }
    thread.reset();
    NES_DEBUG("WindowHandler " << this << ": Thread joinded");
    // TODO what happens to the content of the window that it is still in the state?
    return true;
}

WindowHandler::~WindowHandler() {
    NES_DEBUG("WindowHandler: calling destructor");
    stop();
}

const WindowHandlerPtr createWindowHandler(WindowDefinitionPtr windowDefinition) {
    return std::make_shared<WindowHandler>(windowDefinition);
}

} // namespace NES
