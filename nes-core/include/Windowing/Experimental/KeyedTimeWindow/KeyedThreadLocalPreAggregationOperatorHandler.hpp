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

#ifndef NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDTHREADLOCALPREAGGREGATIONOPERATORHANDLER_HPP_
#define NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDTHREADLOCALPREAGGREGATIONOPERATORHANDLER_HPP_

#include <Runtime/Execution/OperatorHandler.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <memory>
#include <vector>
namespace NES::Experimental {
class HashMapFactory;
using HashMapFactoryPtr = std::shared_ptr<HashMapFactory>;
class LockFreeMultiOriginWatermarkProcessor;
}// namespace NES::Experimental

namespace NES::Windowing::Experimental {
class KeyedThreadLocalSliceStore;
class KeyedSliceStaging;

/**
 * @brief The KeyedThreadLocalPreAggregationOperator provides an operator handler to perform slice-based pre-aggregation of tumbling and sliding windows.
 * This window handler, maintains a slice store for each worker thread and provides them for the aggregation.
 * For each processed tuple buffer triggerThreadLocalState is called, which checks if the thread-local slice store should be triggered.
 */
class KeyedThreadLocalPreAggregationOperatorHandler
    : public Runtime::Execution::OperatorHandler,
      public detail::virtual_enable_shared_from_this<KeyedThreadLocalPreAggregationOperatorHandler, false> {
    using inherited0 = detail::virtual_enable_shared_from_this<KeyedThreadLocalPreAggregationOperatorHandler, false>;
    using inherited1 = Runtime::Reconfigurable;

  public:
    KeyedThreadLocalPreAggregationOperatorHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                                  const std::vector<OriginId> origins,
                                                  std::weak_ptr<KeyedSliceStaging> weakSliceStagingPtr);

    /**
     * @brief Initializes the thread local state for the window operator
     * @param ctx PipelineExecutionContext
     * @param hashmapFactory HashMapFactoryPtr
     */
    void setup(Runtime::Execution::PipelineExecutionContext& ctx, NES::Experimental::HashMapFactoryPtr hashmapFactory);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

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
     * @return
     */
    KeyedThreadLocalSliceStore& getThreadLocalSliceStore(uint64_t workerId);

    /**
     * @brief Returns the window definition.
     * @return Windowing::LogicalWindowDefinitionPtr
     */
    Windowing::LogicalWindowDefinitionPtr getWindowDefinition();

    ~KeyedThreadLocalPreAggregationOperatorHandler();

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override;

  private:
    uint64_t windowSize;
    uint64_t windowSlide;
    std::weak_ptr<KeyedSliceStaging> weakSliceStaging;
    std::vector<std::unique_ptr<KeyedThreadLocalSliceStore>> threadLocalSliceStores;
    std::shared_ptr<::NES::Experimental::LockFreeMultiOriginWatermarkProcessor> watermarkProcessor;
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
    NES::Experimental::HashMapFactoryPtr factory;
};

}// namespace NES::Windowing::Experimental

#endif// NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDTHREADLOCALPREAGGREGATIONOPERATORHANDLER_HPP_
