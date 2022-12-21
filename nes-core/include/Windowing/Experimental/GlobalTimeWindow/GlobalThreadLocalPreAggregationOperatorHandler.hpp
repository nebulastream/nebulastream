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
#ifndef NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEWINDOW_GLOBALTHREADLOCALPREAGGREGATIONOPERATORHANDLER_HPP_
#define NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEWINDOW_GLOBALTHREADLOCALPREAGGREGATIONOPERATORHANDLER_HPP_

#include <Runtime/Execution/OperatorHandler.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalThreadLocalSliceStore.hpp>
namespace NES::Experimental {
class HashMapFactory;
using HashMapFactoryPtr = std::shared_ptr<HashMapFactory>;
class LockFreeMultiOriginWatermarkProcessor;
}// namespace NES::Experimental

namespace NES::Windowing {
class LogicalWindowDefinition;
using LogicalWindowDefinitionPtr = std::shared_ptr<LogicalWindowDefinition>;
}// namespace NES::Windowing

namespace NES::Windowing::Experimental {
class KeyedThreadLocalSliceStore;
class GlobalSliceStaging;

/**
 * @brief The GlobalThreadLocalPreAggregationOperatorHandler provides an operator handler to perform slice-based pre-aggregation
 * of global non-keyed tumbling and sliding windows.
 * This operator handler, maintains a slice store for each worker thread and provides them for the aggregation.
 * For each processed tuple buffer triggerThreadLocalState is called, which checks if the thread-local slice store should be triggered.
 */
class GlobalThreadLocalPreAggregationOperatorHandler
    : public Runtime::Execution::OperatorHandler,
      public detail::virtual_enable_shared_from_this<GlobalThreadLocalPreAggregationOperatorHandler, false> {
    using inherited0 = detail::virtual_enable_shared_from_this<GlobalThreadLocalPreAggregationOperatorHandler, false>;
    using inherited1 = Runtime::Reconfigurable;

  public:
    /**
     * @brief Creates the operator handler with a specific window definition, a set of origins, and access to the slice staging object.
     * @param windowDefinition logical window definition
     * @param origins the set of origins, which can produce data for the window operator
     * @param weakSliceStagingPtr access to the slice staging.
     */
    GlobalThreadLocalPreAggregationOperatorHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                                   const std::vector<OriginId> origins,
                                                   std::weak_ptr<GlobalSliceStaging> weakSliceStagingPtr);

    /**
     * @brief Initializes the thread local state for the window operator
     * @param ctx PipelineExecutionContext
     * @param entrySize Size of the aggregated values in memory
     */
    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    /**
     * @brief Stops the operator handler and triggers all in flight slices.
     * @param pipelineExecutionContext pipeline execution context
     */
    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief This method triggers the thread local state and appends all slices,
     * which end before the current global watermark to the slice staging area.
     * @param wctx WorkerContext
     * @param ctx PipelineExecutionContext
     * @param workerId
     * @param originId
     * @param sequenceNumber
     * @param watermarkTs
     */
    void triggerThreadLocalState(Runtime::WorkerContext& wctx,
                                 Runtime::Execution::PipelineExecutionContext& ctx,
                                 uint64_t workerId,
                                 OriginId originId,
                                 uint64_t sequenceNumber,
                                 uint64_t watermarkTs);

    /**
     * @brief Returns the thread local slice store by a specific worker thread id
     * @param workerId
     * @return GlobalThreadLocalSliceStore
     */
    GlobalThreadLocalSliceStore& getThreadLocalSliceStore(uint64_t workerId);

    /**
     * @brief Returns the window definition.
     * @return Windowing::LogicalWindowDefinitionPtr
     */
    Windowing::LogicalWindowDefinitionPtr getWindowDefinition();

    ~GlobalThreadLocalPreAggregationOperatorHandler();

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override;

  private:
    uint64_t windowSize;
    uint64_t windowSlide;
    std::weak_ptr<GlobalSliceStaging> weakSliceStaging;
    std::vector<std::unique_ptr<GlobalThreadLocalSliceStore>> threadLocalSliceStores;
    std::shared_ptr<::NES::Experimental::LockFreeMultiOriginWatermarkProcessor> watermarkProcessor;
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
    NES::Experimental::HashMapFactoryPtr factory;
};
}// namespace NES::Windowing::Experimental

#endif// NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEWINDOW_GLOBALTHREADLOCALPREAGGREGATIONOPERATORHANDLER_HPP_
