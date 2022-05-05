/*
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

#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_SLICESTAGINGWINDOWHANDLER_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_SLICESTAGINGWINDOWHANDLER_HPP_
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <State/StateVariable.hpp>
#include <Util/Experimental/HashMap.hpp>
#include <Windowing/Experimental/LockFreeMultiOriginWatermarkProcessor.hpp>
#include <Windowing/Experimental/LockFreeWatermarkProcessor.hpp>
#include <Windowing/Experimental/TimeBasedWindow/Events.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedGlobalSliceStore.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedThreadLocalSliceStore.hpp>
#include <Windowing/Experimental/TimeBasedWindow/SliceStaging.hpp>

namespace NES::Windowing::Experimental {

class KeyedThreadLocalSliceStore;

/**
 * @brief The SliceStagingWindowHandler implements a thread local strategy to compute window aggregates for tumbling and sliding windows.
 */
class KeyedSliceMergingOperatorHandler : public Runtime::Execution::OperatorHandler,
                                         public detail::virtual_enable_shared_from_this<KeyedSliceMergingOperatorHandler, false> {
    using inherited0 = detail::virtual_enable_shared_from_this<KeyedSliceMergingOperatorHandler, false>;
    using inherited1 = Runtime::Reconfigurable;

  public:
    KeyedSliceMergingOperatorHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition);

    void setup(Runtime::Execution::PipelineExecutionContext& ctx, NES::Experimental::HashMapFactoryPtr hashmapFactory);

    inline SliceStaging& getSliceStaging() { return *sliceStaging.get(); }

    inline std::weak_ptr<SliceStaging> getSliceStagingPtr() { return sliceStaging; }

    Windowing::LogicalWindowDefinitionPtr getWindowDefinition() { return windowDefinition; };

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    NES::Experimental::Hashmap getHashMap();

    KeyedSlicePtr createKeyedSlice(SliceMergeTask* sliceMergeTask);

    ~KeyedSliceMergingOperatorHandler() { NES_DEBUG("Destruct SliceStagingWindowHandler"); }

  private:
    std::atomic<uint32_t> activeCounter;
    uint64_t windowSize;
    uint64_t windowSlide;
    std::shared_ptr<SliceStaging> sliceStaging;
    std::weak_ptr<KeyedGlobalSliceStore> globalSliceStore;
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
    NES::Experimental::HashMapFactoryPtr factory;
};

}// namespace NES::Windowing::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_SLICESTAGINGWINDOWHANDLER_HPP_
