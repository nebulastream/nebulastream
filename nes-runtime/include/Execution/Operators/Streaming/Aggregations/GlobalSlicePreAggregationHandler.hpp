#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_GLOBALSLICEPREAGGREGATIONHANDLER_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_GLOBALSLICEPREAGGREGATIONHANDLER_HPP_
#include <Runtime/Execution/OperatorHandler.hpp>
#include <vector>
namespace NES::Runtime::Execution::Operators {

class MultiOriginWatermarkProcessor;
class GlobalSliceStaging;
class GlobalThreadLocalSliceStore;
class State;
/**
 * @brief The GlobalThreadLocalPreAggregationOperatorHandler provides an operator handler to perform slice-based pre-aggregation
 * of global non-keyed tumbling and sliding windows.
 * This operator handler, maintains a slice store for each worker thread and provides them for the aggregation.
 * For each processed tuple buffer triggerThreadLocalState is called, which checks if the thread-local slice store should be triggered.
 */
class GlobalSlicePreAggregationHandler : public Runtime::Execution::OperatorHandler,
                                         public detail::virtual_enable_shared_from_this<GlobalSlicePreAggregationHandler, false> {
    using inherited0 = detail::virtual_enable_shared_from_this<GlobalSlicePreAggregationHandler, false>;
    using inherited1 = Runtime::Reconfigurable;

  public:
    /**
     * @brief Creates the operator handler with a specific window definition, a set of origins, and access to the slice staging object.
     * @param windowDefinition logical window definition
     * @param origins the set of origins, which can produce data for the window operator
     * @param weakSliceStagingPtr access to the slice staging.
     */
    GlobalSlicePreAggregationHandler(uint64_t windowSize,
                                     uint64_t windowSlide,
                                     const std::vector<OriginId>& origins,
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
    GlobalThreadLocalSliceStore* getThreadLocalSliceStore(uint64_t workerId);

    ~GlobalSlicePreAggregationHandler();

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override;
    const State* getDefaultState() const;

  private:
    uint64_t windowSize;
    uint64_t windowSlide;
    std::weak_ptr<GlobalSliceStaging> weakSliceStaging;
    std::vector<std::unique_ptr<GlobalThreadLocalSliceStore>> threadLocalSliceStores;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
    std::unique_ptr<State> defaultState;
};
}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_GLOBALSLICEPREAGGREGATIONHANDLER_HPP_
