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

#ifndef NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEBASEDWINDOW_GLOBALSLIDINGWINDOWSINKOPERATORHANDLER_HPP_
#define NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEBASEDWINDOW_GLOBALSLIDINGWINDOWSINKOPERATORHANDLER_HPP_

#include <Runtime/Execution/OperatorHandler.hpp>
#include <Windowing/Experimental/GlobalSliceStore.hpp>

namespace NES::Experimental {
class HashMapFactory;
using HashMapFactoryPtr = std::shared_ptr<HashMapFactory>;
class LockFreeMultiOriginWatermarkProcessor;
}// namespace NES::Experimental

namespace NES::Windowing::Experimental {
class KeyedThreadLocalSliceStore;
class WindowTriggerTask;
class GlobalSlice;
using GlobalSlicePtr = std::unique_ptr<GlobalSlice>;
using GlobalSliceSharedPtr = std::shared_ptr<GlobalSlice>;

/**
 * @brief The GlobalSlidingWindowSinkOperatorHandler.
 */
class GlobalSlidingWindowSinkOperatorHandler
    : public Runtime::Execution::OperatorHandler,
      public detail::virtual_enable_shared_from_this<GlobalSlidingWindowSinkOperatorHandler, false> {
    using inherited0 = detail::virtual_enable_shared_from_this<GlobalSlidingWindowSinkOperatorHandler, false>;
    using inherited1 = Runtime::Reconfigurable;

  public:
    GlobalSlidingWindowSinkOperatorHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                           std::shared_ptr<GlobalSliceStore<GlobalSlice>>& globalSliceStore);

    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize);

    Windowing::LogicalWindowDefinitionPtr getWindowDefinition();

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    GlobalSlicePtr createGlobalSlice(WindowTriggerTask* sliceMergeTask);

    std::vector<GlobalSliceSharedPtr> getSlicesForWindow(WindowTriggerTask* windowTriggerTask);

    GlobalSliceStore<GlobalSlice>& getGlobalSliceStore() { return *globalSliceStore; }

    ~GlobalSlidingWindowSinkOperatorHandler() { NES_DEBUG("Destruct GlobalSlidingWindowSinkOperatorHandler"); }

  private:
    uint64_t windowSize;
    uint64_t windowSlide;
    uint64_t entrySize;
    std::shared_ptr<GlobalSliceStore<GlobalSlice>> globalSliceStore;
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
    NES::Experimental::HashMapFactoryPtr factory;
};

}// namespace NES::Windowing::Experimental

#endif//NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEBASEDWINDOW_GLOBALSLIDINGWINDOWSINKOPERATORHANDLER_HPP_
