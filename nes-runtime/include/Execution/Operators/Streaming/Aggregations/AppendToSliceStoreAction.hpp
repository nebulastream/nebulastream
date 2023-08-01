#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_APPEND_TO_SLICE_STORE_ACTION_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_APPEND_TO_SLICE_STORE_ACTION_HPP_
#include "Runtime/Execution/OperatorHandler.hpp"
#include <Execution/Operators/Streaming/Aggregations/SliceMergingAction.hpp>
#include <Execution/Operators/Streaming/Aggregations/SlidingWindowSliceStore.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <memory>
namespace NES::Runtime::Execution::Operators {
class MultiOriginWatermarkProcessor;

template<class Slice>
class AppendToSliceStoreHandler : public Runtime::Execution::OperatorHandler {
  public:
    AppendToSliceStoreHandler(uint64_t windowSize, uint64_t windowSlide);
    void start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) override {}
    void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;
    void appendToGlobalSliceStore(std::unique_ptr<Slice> slice);
    void triggerSlidingWindows(Runtime::WorkerContext& wctx,
                               Runtime::Execution::PipelineExecutionContext& ctx,
                               uint64_t sequenceNumber,
                               uint64_t slideEnd);

  private:
    std::unique_ptr<SlidingWindowSliceStore<Slice>> sliceStore;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
    std::atomic<uint64_t> lastTriggerWatermark = 0;
    std::atomic<uint64_t> resultSequenceNumber = 1;
    std::mutex triggerMutex;
};

template<class Slice>
class AppendToSliceStoreAction : public SliceMergingAction {
  public:
    AppendToSliceStoreAction(const uint64_t operatorHandlerIndex);

    void emitSlice(ExecutionContext& ctx,
                   ExecuteOperatorPtr& child,
                   Value<UInt64>& windowStart,
                   Value<UInt64>& windowEnd,
                   Value<UInt64>& sequenceNumber,
                   Value<MemRef>& globalSlice) const override;

  private:
    const uint64_t operatorHandlerIndex;
};
class NonKeyedSlice;
using NonKeyedAppendToSliceStoreAction = AppendToSliceStoreAction<NonKeyedSlice>;
using NonKeyedAppendToSliceStoreHandler = AppendToSliceStoreHandler<NonKeyedSlice>;
class KeyedSlice;
using KeyedAppendToSliceStoreAction = AppendToSliceStoreAction<KeyedSlice>;
using KeyedAppendToSliceStoreHandler = AppendToSliceStoreHandler<KeyedSlice>;
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_APPEND_TO_SLICE_STORE_ACTION_HPP_
