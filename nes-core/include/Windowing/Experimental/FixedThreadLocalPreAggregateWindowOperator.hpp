#ifndef NES_BENCHMARK_INCLUDE_MICROBENCHMARKS_WINDOWING_FIXEDTHREADLOCALPREAGGREGATEWINDOWOPERATOR_HPP_
#define NES_BENCHMARK_INCLUDE_MICROBENCHMARKS_WINDOWING_FIXEDTHREADLOCALPREAGGREGATEWINDOWOPERATOR_HPP_

#include <MicroBenchmarks/Windowing/WindowOperatorPipeline.hpp>
#include <MicroBenchmarks/Windowing/robin_hood.h>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <State/StateVariable.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowedJoinSliceListStore.hpp>
#include <Windowing/WindowActions/BaseExecutableJoinAction.hpp>
#include <Windowing/WindowActions/ExecutableCompleteAggregationTriggerAction.hpp>
#include <Windowing/WindowActions/ExecutableNestedLoopJoinTriggerAction.hpp>
#include <Windowing/WindowActions/ExecutableSliceAggregationTriggerAction.hpp>
#include <Windowing/WindowAggregations/ExecutableAVGAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableCountAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableMaxAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableMedianAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableMinAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <Windowing/WindowHandler/AggregationWindowHandler.hpp>
#include <Windowing/WindowHandler/JoinHandler.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <Windowing/WindowPolicies/ExecutableOnTimeTriggerPolicy.hpp>
#include <Windowing/WindowPolicies/ExecutableOnWatermarkChangeTriggerPolicy.hpp>
namespace NES {

// A single slice, which contains all keys
template <std::uint64_t size> class FixedKeyedSlice {
public:
  FixedKeyedSlice(uint64_t start, uint64_t end) : start(start), end(end){};
  const uint64_t start;
  const uint64_t end;
  std::array<uint64_t, size> map;
};

// A slice store, which contains a set of slices
template <std::uint64_t size> class FixedSliceStore {
public:
  FixedKeyedSlice<size> &findSliceByTs(uint64_t ts) {

    if (slices.empty()) {
      slices.emplace_back(0, sliceSize);
    }

    for (uint64_t i = slices.size() - 1; i >= 0; i--) {
      auto &slice = slices[i];
      if (ts >= slice.start) {
        return slice;
      }
    }
    auto &lastSlice = last();

    return slices.emplace_back(lastSlice.end, lastSlice.end + sliceSize);
  }
  FixedKeyedSlice<size> &first() { return slices[0]; }
  FixedKeyedSlice<size> &last() { return slices[slices.size() - 1]; }
  const uint64_t sliceSize = 100;
  std::vector<FixedKeyedSlice<size>> slices{FixedKeyedSlice<size>(1, 100)};
};

/**
 * @brief The Thread local window handler.
 * threadLocalSliceStore scopes aggregates by Thread|Slice|Key.
 * If the watermark passes a slice end, thread local slices are merged into the
 * globalSliceStore scoped by Slice|Key
 */
template <class KeyType, class InputType, class PartialAggregateType,
          class FinalAggregateType, std::uint64_t size>
class FixedThreadLocalWindowHandler : public Windowing::AbstractWindowHandler {
public:
  FixedThreadLocalWindowHandler(
      const Windowing::LogicalWindowDefinitionPtr &windowDefinition)
      : AbstractWindowHandler(windowDefinition) {}
  bool start(Runtime::StateManagerPtr, uint32_t) override { return true; }
  bool stop() override { return true; }

  void trigger(Runtime::WorkerContext &, bool) override {}

  bool setup(Runtime::Execution::PipelineExecutionContextPtr) override {
    return false;
  }

  Windowing::WindowManagerPtr getWindowManager() override {
    return NES::Windowing::WindowManagerPtr();
  }

  std::shared_ptr<Windowing::MultiOriginWatermarkProcessor>
  getWatermarkProcessor() {
    return watermarkProcessor;
  }

  std::string toString() override { return std::string(); }

  std::array<FixedSliceStore<size>, size> threadLocalSliceStore;
  FixedSliceStore<size> globalSliceStore;
};
template <std::uint64_t size>
class FixedThreadLocalPreAggregateWindowOperator
    : public Runtime::Execution::ExecutablePipelineStage {
public:
  uint32_t setup(Runtime::Execution::PipelineExecutionContext
                     &pipelineExecutionContext) override {
    {
      auto windowOperatorHandler =
          pipelineExecutionContext
              .getOperatorHandler<Windowing::WindowOperatorHandler>(0);
      auto windowDefinition = windowOperatorHandler->getWindowDefinition();
      auto windowHandler = std::make_shared<FixedThreadLocalWindowHandler<
          int64_t, int64_t, int64_t, int64_t, size>>(windowDefinition);
      windowOperatorHandler->setWindowHandler(windowHandler);
      windowHandler->setup(pipelineExecutionContext.shared_from_this());
    };
    return 0;
  }
  __attribute__((noinline)) ExecutionResult execute(
      Runtime::TupleBuffer &inputTupleBuffer,
      Runtime::Execution::PipelineExecutionContext &pipelineExecutionContext,
      Runtime::WorkerContext &workerContext) override {

    auto inputTuples = inputTupleBuffer.getBuffer<Record>();
    auto windowOperatorHandler =
        pipelineExecutionContext
            .getOperatorHandler<Windowing::WindowOperatorHandler>(0);
    auto abstractWindowHandler = windowOperatorHandler->getWindowHandler();

    auto windowHandler = std::static_pointer_cast<FixedThreadLocalWindowHandler<
        uint64_t, uint64_t, uint64_t, uint64_t, size>>(abstractWindowHandler);

    auto windowManager = windowHandler->getWindowManager();
    auto allowedLateness = 0;
    auto workerId = workerContext.getId();
    auto threadLocalSliceStore = windowHandler->threadLocalSliceStore[workerId];
    auto records = inputTupleBuffer.getNumberOfTuples();
    uint64_t currentWatermark = 0;
    for (uint64_t recordIndex = 0; recordIndex < records; ++recordIndex) {
      uint64_t key = inputTuples[recordIndex].id % size;
      auto current_ts = inputTuples[recordIndex].timestamp;

      currentWatermark =
          std::max(currentWatermark, current_ts - allowedLateness);
      // pre aggregate in local slice store
      auto& slice = threadLocalSliceStore.findSliceByTs(current_ts);

      slice.map[key] = slice.map[key] + inputTuples[recordIndex].value;
    };

    // update the global watermark, and merge all local slides.
    {
      // TODO this operation should be atomic, till it is we have to get a lock
      // here
      auto watermarkProcessor = windowHandler->getWatermarkProcessor();
      watermarkProcessor->updateWatermark(currentWatermark,
                                          inputTupleBuffer.getSequenceNumber(),
                                          inputTupleBuffer.getOriginId());
      auto newWatermark = watermarkProcessor->getCurrentWatermark();
      if (windowHandler->getLastWatermark() < newWatermark) {
        for (auto const &localLiceStore :
             windowHandler->threadLocalSliceStore) {
          // iterate about all slices in this slice store
          // todo maybe we can infer the index directly and omit the
          // looping/check?
          for (auto const &slice : localLiceStore.slices) {
            std::cout << slice.start << " - " << slice.end << std::endl;
          }
        }
      }
    }

    return ExecutionResult::Ok;
  };
};
} // namespace NES

#endif // NES_BENCHMARK_INCLUDE_MICROBENCHMARKS_WINDOWING_ROBBINTHREADLOCALPREAGGREGATEWINDOWOPERATOR_HPP_
