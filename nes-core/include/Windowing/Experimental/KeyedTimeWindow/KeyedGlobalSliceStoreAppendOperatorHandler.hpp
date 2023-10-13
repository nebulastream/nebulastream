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

#ifndef NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDGLOBALSLICESTOREAPPENDOPERATORHANDLER_HPP_
#define NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDGLOBALSLICESTOREAPPENDOPERATORHANDLER_HPP_
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>

namespace NES::Experimental {
class HashMapFactory;
using HashMapFactoryPtr = std::shared_ptr<HashMapFactory>;
class LockFreeMultiOriginWatermarkProcessor;
}// namespace NES::Experimental

namespace NES::Windowing::Experimental {
class KeyedThreadLocalSliceStore;
class KeyedSlice;
using KeyedSlicePtr = std::unique_ptr<KeyedSlice>;
template<typename SliceType>
class GlobalSliceStore;

/**
 * @brief Operator handler, which appends merged slices to the global slice store.
 * This operator is only used for sliding windows, as we can skip the global slice store for tumbling windows.s
 */
class KeyedGlobalSliceStoreAppendOperatorHandler
    : public Runtime::Execution::OperatorHandler,
      public detail::virtual_enable_shared_from_this<KeyedGlobalSliceStoreAppendOperatorHandler, false> {
    using inherited0 = detail::virtual_enable_shared_from_this<KeyedGlobalSliceStoreAppendOperatorHandler, false>;
    using inherited1 = Runtime::Reconfigurable;

  public:
    /**
     * @brief Creates the operator handler.
     * @param windowDefinition logical window definition
     * @param globalSliceStore reference to the global slice store
     */
    KeyedGlobalSliceStoreAppendOperatorHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                               std::weak_ptr<GlobalSliceStore<KeyedSlice>> globalSliceStore);

    ~KeyedGlobalSliceStoreAppendOperatorHandler();

    /**
     * @brief Returns the logical window definition
     * @return Windowing::LogicalWindowDefinitionPtr
     */
    Windowing::LogicalWindowDefinitionPtr getWindowDefinition();

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    /**
     * @brief Stops the operator handler and triggers for all slices in the global slice store the associated windows.
     * @param pipelineExecutionContext
     */
    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief This function triggers the slice merging and appends the merged slice to the global slice store.
     * Based on this function, we can trigger the final window aggregation, for sliding windows.
     * @param wctx WorkerContext
     * @param ctx PipelineExecutionContext
     * @param sequenceNumber
     * @param mergedSlice
     */
    void triggerSliceMerging(Runtime::WorkerContext& wctx,
                             Runtime::Execution::PipelineExecutionContext& ctx,
                             uint64_t sequenceNumber,
                             KeyedSlicePtr mergedSlice);

  private:
    uint64_t windowSize;
    uint64_t windowSlide;
    std::weak_ptr<GlobalSliceStore<KeyedSlice>> globalSliceStore;
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
    NES::Experimental::HashMapFactoryPtr factory;
};

}// namespace NES::Windowing::Experimental

#endif// NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDGLOBALSLICESTOREAPPENDOPERATORHANDLER_HPP_
