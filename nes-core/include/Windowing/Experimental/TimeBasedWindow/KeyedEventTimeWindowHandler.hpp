#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDEVENTTIMEWINDOWHANDLER_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDEVENTTIMEWINDOWHANDLER_HPP_

#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/WorkerContext.hpp>
#include <State/StateVariable.hpp>
#include <Windowing/Experimental/LockFreeWatermarkProcessor.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedThreadLocalSliceStore.hpp>
#include <Windowing/Experimental/TimeBasedWindow/SliceStaging.hpp>
#include <Windowing/Experimental/HashMap.hpp>


namespace NES::Windowing::Experimental {
class KeyedThreadLocalSliceStore;

class SliceMergeTask {
  public:
    uint64_t sliceIndex;
};

class KeyedEventTimeWindowHandler : public Runtime::Execution::OperatorHandler, public detail::virtual_enable_shared_from_this<KeyedEventTimeWindowHandler, false> {
    using inherited0 = detail::virtual_enable_shared_from_this<KeyedEventTimeWindowHandler, false>;
    using inherited1 = Runtime::Reconfigurable;

  public:
    KeyedEventTimeWindowHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition);

    void setup(Runtime::Execution::PipelineExecutionContext& ctx, NES::Experimental::HashMapFactoryPtr hashmapFactory);

    void triggerThreadLocalState(Runtime::WorkerContext& wctx,
                                 Runtime::Execution::PipelineExecutionContext& ctx,
                                 uint64_t workerId,
                                 uint64_t originId,
                                 uint64_t sequenceNumber,
                                 uint64_t watermarkTs);

    KeyedThreadLocalSliceStore& getThreadLocalSliceStore(uint64_t workerId) { return threadLocalSliceStores[workerId]; }

    inline SliceStaging& getSliceStaging() { return sliceStaging; }

    Windowing::LogicalWindowDefinitionPtr getWindowDefinition();

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;
    void stop(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;
    NES::Experimental::Hashmap getHashMap();
    KeyedSlicePtr createKeyedSlice(uint64_t sliceIndex);

  private:
    std::atomic<bool> isRunning;
    uint64_t sliceSize;
    std::vector<KeyedThreadLocalSliceStore> threadLocalSliceStores;
    SliceStaging sliceStaging;
    std::shared_ptr<::NES::Experimental::LockFreeMultiOriginWatermarkProcessor> watermarkProcessor;
    const Windowing::LogicalWindowDefinitionPtr windowDefinition;
    NES::Experimental::HashMapFactoryPtr factory;
};

}// namespace NES::Windowing::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDEVENTTIMEWINDOWHANDLER_HPP_
