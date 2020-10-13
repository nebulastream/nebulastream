#ifndef INCLUDE_WINDOWS_WINDOW_HPP_
#define INCLUDE_WINDOWS_WINDOW_HPP_

#include <Util/Logger.hpp>
#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>

#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <cstring>

#include <State/StateManager.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/CountAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <Windowing/WindowAggregations/MaxAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MinAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <atomic>
#include <NodeEngine/QueryManager.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/Runtime/WindowHandler.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <atomic>
#include <iostream>
#include <memory>
#include <utility>
#include <unistd.h>

namespace NES {
class QueryManager;
typedef std::shared_ptr<QueryManager> QueryManagerPtr;

class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;


class WindowHandler;
typedef std::shared_ptr<WindowHandler> WindowHandlerPtr;

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

template<class KeyType, class InputType, class FinalAggregateType, class PartialAggregateType>
class WindowHandlerImpl;

class WindowHandler : std::enable_shared_from_this<WindowHandler>{

  public:

    WindowHandler(LogicalWindowDefinitionPtr windowDefinition);

    template<class KeyType, class InputType, class FinalAggregateType, class PartialAggregateType>
    auto as(){
         return std::dynamic_pointer_cast<WindowHandlerImpl<KeyType, InputType, FinalAggregateType, PartialAggregateType>>(shared_from_this());
    }

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
    virtual bool setup(QueryManagerPtr queryManager, BufferManagerPtr bufferManager, PipelineStagePtr nextPipeline, uint32_t pipelineStageId) = 0;

    virtual void* getWindowState() = 0;
    WindowManagerPtr getWindowManager() { return windowManager; };

    [[nodiscard]] uint64_t getOriginId() const;

    void setOriginId(uint64_t originId);

  protected:
    std::atomic_bool running{false};
    LogicalWindowDefinitionPtr windowDefinition;
    WindowManagerPtr windowManager;
    PipelineStagePtr nextPipeline;
    std::shared_ptr<std::thread> thread;
    std::mutex runningTriggerMutex;
    uint32_t pipelineStageId;
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    uint64_t originId;

    MemoryLayoutPtr windowTupleLayout;
    SchemaPtr windowTupleSchema;

};





}// namespace NES
#endif /* INCLUDE_WINDOWS_WINDOW_HPP_ */
