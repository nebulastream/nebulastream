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

#ifndef NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_NONKEYEDTIMEWINDOW_NONKEYEDSLIDINGWINDOWSINKOPERATORHANDLER_HPP_
#define NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_NONKEYEDTIMEWINDOW_NONKEYEDSLIDINGWINDOWSINKOPERATORHANDLER_HPP_

#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
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
class NonKeyedSlice;
using NonKeyedSlicePtr = std::unique_ptr<NonKeyedSlice>;
using NonKeyedSliceSharedPtr = std::shared_ptr<NonKeyedSlice>;

/**
 * @brief Operator handler for the final window sink of global (non-keyed) sliding windows.
 * This window handler maintains the global slice store and allows the window operator to trigger individual windows.
 */
class NonKeyedSlidingWindowSinkOperatorHandler
    : public Runtime::Execution::OperatorHandler,
      public detail::virtual_enable_shared_from_this<NonKeyedSlidingWindowSinkOperatorHandler, false> {
    using inherited0 = detail::virtual_enable_shared_from_this<NonKeyedSlidingWindowSinkOperatorHandler, false>;
    using inherited1 = Runtime::Reconfigurable;

  public:
    /**
     * @brief Constructor for the operator handler.
     * @param windowDefinition
     * @param globalSliceStore
     */
    NonKeyedSlidingWindowSinkOperatorHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                             std::shared_ptr<GlobalSliceStore<NonKeyedSlice>>& globalSliceStore);

    /**
     * @brief Initializes the operator handler.
     * @param ctx reference to the pipeline context.
     * @param entrySize the size of the aggregated values.
     */
    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Returns the logical window definition
     * @return Windowing::LogicalWindowDefinitionPtr
     */
    Windowing::LogicalWindowDefinitionPtr getWindowDefinition();

    /**
     * @brief Creates a new global slice.
     * This is used to create the state for a specific window from the generated code.
     * @param windowTriggerTask WindowTriggerTask to identify the start and end ts of the window
     * @return GlobalSlicePtr
     */
    NonKeyedSlicePtr createGlobalSlice(WindowTriggerTask* windowTriggerTask);

    /**
     * @brief This function accesses the global slice store and returns a list of slices,
     * which are covered by the window specified in the windowTriggerTask
     * @param windowTriggerTask identifies the window, which we want to trigger.
     * @return std::vector<GlobalSliceSharedPtr> list of global slices.
     */
    std::vector<NonKeyedSliceSharedPtr> getSlicesForWindow(WindowTriggerTask* windowTriggerTask);

    /**
     * @brief Returns a reference to the global slice store. This should only be used by the generated code,
     * to access functions on the global slice sotre.
     * @return GlobalSliceStore<GlobalSlice>&
     */
    GlobalSliceStore<NonKeyedSlice>& getGlobalSliceStore();

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override;

    ~NonKeyedSlidingWindowSinkOperatorHandler();

  private:
    uint64_t entrySize;
    std::shared_ptr<GlobalSliceStore<NonKeyedSlice>> globalSliceStore;
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
    NES::Experimental::HashMapFactoryPtr factory;
};

}// namespace NES::Windowing::Experimental

#endif// NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_NONKEYEDTIMEWINDOW_NONKEYEDSLIDINGWINDOWSINKOPERATORHANDLER_HPP_
