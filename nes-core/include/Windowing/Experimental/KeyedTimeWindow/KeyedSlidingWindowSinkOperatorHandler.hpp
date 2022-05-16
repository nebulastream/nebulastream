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

#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDSLIDINGWINDOWSINKOPERATORHANDLER_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDSLIDINGWINDOWSINKOPERATORHANDLER_HPP_

#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::Experimental {
class HashMapFactory;
using HashMapFactoryPtr = std::shared_ptr<HashMapFactory>;
class LockFreeMultiOriginWatermarkProcessor;
}// namespace NES::Experimental

namespace NES::Windowing::Experimental {
class KeyedThreadLocalSliceStore;
class WindowTriggerTask;
class KeyedSlice;
using KeyedSlicePtr = std::unique_ptr<KeyedSlice>;
using KeyedSliceSharedPtr = std::shared_ptr<KeyedSlice>;
template<typename SliceType>
class GlobalSliceStore;

/**
 * @brief The KeyedSlidingWindowSinkOperatorHandler.
 */
class KeyedSlidingWindowSinkOperatorHandler
    : public Runtime::Execution::OperatorHandler,
      public detail::virtual_enable_shared_from_this<KeyedSlidingWindowSinkOperatorHandler, false> {
    using inherited0 = detail::virtual_enable_shared_from_this<KeyedSlidingWindowSinkOperatorHandler, false>;
    using inherited1 = Runtime::Reconfigurable;

  public:
    KeyedSlidingWindowSinkOperatorHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                          std::shared_ptr<GlobalSliceStore<KeyedSlice>>& globalSliceStore);

    void setup(Runtime::Execution::PipelineExecutionContext& ctx, NES::Experimental::HashMapFactoryPtr hashmapFactory);

    Windowing::LogicalWindowDefinitionPtr getWindowDefinition();

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    KeyedSlicePtr createKeyedSlice(WindowTriggerTask* sliceMergeTask);

    std::vector<KeyedSliceSharedPtr> getSlicesForWindow(WindowTriggerTask* windowTriggerTask);

    GlobalSliceStore<KeyedSlice>& getGlobalSliceStore() { return *globalSliceStore; }

    ~KeyedSlidingWindowSinkOperatorHandler() { NES_DEBUG("Destruct KeyedEventTimeWindowHandler"); }

  private:
    uint64_t windowSize;
    uint64_t windowSlide;
    std::shared_ptr<GlobalSliceStore<KeyedSlice>> globalSliceStore;
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
    NES::Experimental::HashMapFactoryPtr factory;
};

}// namespace NES::Windowing::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDSLIDINGWINDOWSINKOPERATORHANDLER_HPP_
