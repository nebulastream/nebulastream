#ifndef INCLUDE_WINDOWS_WINDOW_HPP_
#define INCLUDE_WINDOWS_WINDOW_HPP_

#include <Util/Logger.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <utility>

namespace NES::Windowing {


/**
 * @brief The window handler checks every n seconds if a window should be triggered.
 * It has knowledge about the window definition and performs final aggregation.
 */
class AbstractWindowHandler {
  public:
//    template<class KeyType, class InputType, class FinalAggregateType, class PartialAggregateType>
//    auto as() {
//        return std::dynamic_pointer_cast<WindowHandlerImpl<KeyType, InputType, FinalAggregateType, PartialAggregateType>>(shared_from_this());
//    }

    /**
    * @brief Starts thread to check if the window should be triggered.
    * @return boolean if the window thread is started
    */
    virtual bool start() = 0;

    /**
     * @brief Stops the window thread.
     * @return
     */
    virtual bool stop() = 0;

    /**
     * @brief triggers all ready windows.
     * @return
     */
    virtual void trigger() = 0;

    /**
     * @brief updates all maxTs in all stores
     * @param ts
     * @param originId
     */
    virtual void updateAllMaxTs(uint64_t ts, uint64_t originId) = 0;

    /**
    * @brief Initialises the state of this window depending on the window definition.
    */
    virtual bool setup(QueryManagerPtr queryManager, BufferManagerPtr bufferManager, PipelineStagePtr nextPipeline, uint32_t pipelineStageId, uint64_t originId) = 0;

    /**
     * @brief Returns the window state, as a untyped pointer to the state variable.
     * @deprecated todo remove this untyped call.
     * @return void* to state variable.
     */
    virtual void* getWindowState() = 0;

    /**
     * @brief Returns window manager.
     * @return WindowManager.
     */
    virtual WindowManagerPtr getWindowManager() = 0;

  protected:
    std::atomic_bool running{false};
    WindowManagerPtr windowManager;
    PipelineStagePtr nextPipeline;
    std::shared_ptr<std::thread> thread;
    std::mutex runningTriggerMutex;
    uint32_t pipelineStageId;
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    uint64_t originId;

    SchemaPtr windowTupleSchema;
};

}// namespace NES::Windowing
#endif /* INCLUDE_WINDOWS_WINDOW_HPP_ */
