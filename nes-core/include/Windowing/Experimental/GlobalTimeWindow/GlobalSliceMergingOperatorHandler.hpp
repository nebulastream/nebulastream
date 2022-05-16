#ifndef NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEBASEDWINDOW_GLOBALSLICEMERGINGOPERATORHANDLER_HPP_
#define NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEBASEDWINDOW_GLOBALSLICEMERGINGOPERATORHANDLER_HPP_
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::Windowing::Experimental {
class SliceMergeTask;
class GlobalSlice;
using GlobalSlicePtr = std::unique_ptr<GlobalSlice>;
class GlobalSliceStaging;

/**
 * @brief The SliceStagingWindowHandler implements a thread local strategy to compute window aggregates for tumbling and sliding windows.
 */
class GlobalSliceMergingOperatorHandler : public Runtime::Execution::OperatorHandler,
                                         public detail::virtual_enable_shared_from_this<GlobalSliceMergingOperatorHandler, false> {
    using inherited0 = detail::virtual_enable_shared_from_this<GlobalSliceMergingOperatorHandler, false>;
    using inherited1 = Runtime::Reconfigurable;

  public:
    GlobalSliceMergingOperatorHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition);

    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize);

    /**
     * @brief Get a reference to the slice staging.
     * @note This should be only called from the generated code.
     * @return SliceStaging
     */
    inline GlobalSliceStaging& getSliceStaging() { return *sliceStaging.get(); }

    /**
     * @brief Gets a weak pointer to the slice staging
     * @return std::weak_ptr<SliceStaging>
     */
    std::weak_ptr<GlobalSliceStaging> getSliceStagingPtr();


    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Creates a new keyed slice for a specific slice merge task
     * @param sliceMergeTaskS liceMergeTask
     * @return KeyedSlicePtr
     */
    GlobalSlicePtr createGlobalSlice(SliceMergeTask* sliceMergeTask);

    /**
     * @brief Gets the window definition
     * @return Windowing::LogicalWindowDefinitionPtr
     */
    Windowing::LogicalWindowDefinitionPtr getWindowDefinition();

    ~GlobalSliceMergingOperatorHandler();

  private:
    std::atomic<uint32_t> activeCounter;
    uint64_t windowSize;
    uint64_t windowSlide;
    uint64_t entrySize;
    std::shared_ptr<GlobalSliceStaging> sliceStaging;
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
};

}// namespace NES::Windowing::Experimental
#endif//NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEBASEDWINDOW_GLOBALSLICEMERGINGOPERATORHANDLER_HPP_
