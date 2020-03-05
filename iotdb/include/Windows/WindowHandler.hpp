#ifndef INCLUDE_WINDOWS_WINDOW_HPP_
#define INCLUDE_WINDOWS_WINDOW_HPP_

#include <thread>
#include <QueryLib/WindowManagerLib.hpp>
#include <API/Window/WindowDefinition.hpp>
#include <NodeEngine/BufferManager.hpp>

namespace NES {
class WindowHandler;
typedef std::shared_ptr<WindowHandler> WindowHandlerPtr;

class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

/**
 * @brief This represents a window during query execution.
 */
class WindowHandler {
  public:
    WindowHandler() = default;
    WindowHandler(WindowDefinitionPtr windowDefinition);
    ~WindowHandler();

    /**
     * @brief Initialises the state of this window depending on the window definition.
     */
    bool setup(QueryExecutionPlanPtr queryExecutionPlan, uint32_t pipelineStageId);

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
    void aggregateWindows(WindowSliceStore<PartialAggregateType>* store,
                          WindowDefinitionPtr windowDefinition,
                          TupleBufferPtr tupleBuffer);

    void* getWindowState();
    WindowManagerPtr getWindowManager() {
        return windowManager;
    };

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {}

  private:
    bool running = false;
    WindowDefinitionPtr windowDefinition;
    WindowManagerPtr windowManager;
    void* windowState;
    std::thread thread;
    uint32_t pipelineStageId;
    QueryExecutionPlanPtr queryExecutionPlan;
};

//just for test compability
const WindowHandlerPtr createTestWindow(size_t campainCnt, size_t windowSizeInSec);
const WindowHandlerPtr createWindowHandler(WindowDefinitionPtr windowDefinition);

} // namespace NES
#endif /* INCLUDE_WINDOWS_WINDOW_HPP_ */
