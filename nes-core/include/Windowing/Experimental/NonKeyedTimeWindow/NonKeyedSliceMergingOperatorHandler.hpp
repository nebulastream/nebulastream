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
#ifndef NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_NONKEYEDTIMEWINDOW_NONKEYEDSLICEMERGINGOPERATORHANDLER_HPP_
#define NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_NONKEYEDTIMEWINDOW_NONKEYEDSLICEMERGINGOPERATORHANDLER_HPP_
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>

namespace NES::Windowing::Experimental {
class SliceMergeTask;
class NonKeyedSlice;
using NonKeyedSlicePtr = std::unique_ptr<NonKeyedSlice>;
class NonKeyedSliceStaging;

/**
 * @brief The GlobalSliceMergingOperatorHandler merges thread local pre-aggregated slices for global
 * tumbling and sliding window aggregations.
 */
class NonKeyedSliceMergingOperatorHandler
    : public Runtime::Execution::OperatorHandler,
      public detail::virtual_enable_shared_from_this<NonKeyedSliceMergingOperatorHandler, false> {
    using inherited0 = detail::virtual_enable_shared_from_this<NonKeyedSliceMergingOperatorHandler, false>;
    using inherited1 = Runtime::Reconfigurable;

  public:
    /**
     * @brief Constructor for the GlobalSliceMergingOperatorHandler
     * @param windowDefinition
     */
    NonKeyedSliceMergingOperatorHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition);

    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Get a reference to the slice staging.
     * @note This should be only called from the generated code.
     * @return KeyedSliceStaging
     */
    inline NonKeyedSliceStaging& getSliceStaging() { return *sliceStaging.get(); }

    /**
     * @brief Gets a weak pointer to the slice staging
     * @return std::weak_ptr<KeyedSliceStaging>
     */
    std::weak_ptr<NonKeyedSliceStaging> getSliceStagingPtr();

    /**
     * @brief Creates a new global slice for a specific slice merge task
     * @param sliceMergeTask SliceMergeTask
     * @return GlobalSlicePtr
     */
    NonKeyedSlicePtr createGlobalSlice(SliceMergeTask* sliceMergeTask);

    /**
     * @brief Gets the window definition
     * @return Windowing::LogicalWindowDefinitionPtr
     */
    Windowing::LogicalWindowDefinitionPtr getWindowDefinition();

    ~NonKeyedSliceMergingOperatorHandler();

  private:
    uint64_t entrySize;
    std::shared_ptr<NonKeyedSliceStaging> sliceStaging;
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
};

}// namespace NES::Windowing::Experimental
#endif// NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_NONKEYEDTIMEWINDOW_NONKEYEDSLICEMERGINGOPERATORHANDLER_HPP_
