#ifndef NES_BENCHMARK_INCLUDE_MICROBENCHMARKS_WINDOWING_ROBBINTHREADLOCALPREAGGREGATEWINDOWOPERATOR_HPP_
#define NES_BENCHMARK_INCLUDE_MICROBENCHMARKS_WINDOWING_ROBBINTHREADLOCALPREAGGREGATEWINDOWOPERATOR_HPP_

#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <State/StateVariable.hpp>
#include <Util/Logger.hpp>
#include <Windowing/Experimental/robin_hood.h>
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

namespace NES::Experimental {

/**
 * @brief The ResultRecord of a window aggregation
 */
struct WindowResultRecord {
    uint64_t windowStart;
    uint64_t windowEnd;
    uint64_t key;
    uint64_t aggregateValue;
};

// A single slice, which contains all keys
class KeyedSlice {
  public:
    KeyedSlice(uint64_t start, uint64_t end) : start(start), end(end){};
    const uint64_t start;
    const uint64_t end;
    robin_hood::unordered_flat_map<uint64_t, uint64_t> map;
};

// A slice store, which contains a set of slices
class SliceStore {
  public:
    KeyedSlice& findSliceByTs(uint64_t ts) {

        if (slices.empty()) {
            slices.emplace_back(0, sliceSize);
        }

        for (uint64_t i = slices.size() - 1; i >= 0; i--) {
            auto& slice = slices[i];
            if (ts >= slice.start) {
                return slice;
            }
        }
        auto& lastSlice = last();

        return slices.emplace_back(lastSlice.end, lastSlice.end + sliceSize);
    }
    KeyedSlice& first() { return slices[0]; }
    KeyedSlice& last() { return slices[slices.size() - 1]; }
    const uint64_t sliceSize = 100;
    std::vector<KeyedSlice> slices{};
};

/**
 * @brief The Thread local window handler.
 * ThreadLocalSliceStore scopes aggregates by Thread|Slice|Key.
 * If the watermark passes a slice end, thread local slices are merged into the globalSliceStore scoped by Slice|Key
 *
 * For tumbling window:
 * each window = one slice
 * # calculate the number of slices
 * numberOfSlices = allowedLateness / windowSize
 * # get the slice index for a particular ts.
 * sliceIndex = (ts % sliceSize) % numberOfSlices
 */
template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class ThreadLocalWindowHandler : public Windowing::AbstractWindowHandler {
  public:
    ThreadLocalWindowHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition)
        : AbstractWindowHandler(windowDefinition) {}
    bool start(Runtime::StateManagerPtr, uint32_t) override { return true; }
    bool stop() override { return true; }

    void trigger(Runtime::WorkerContext&, bool) override {}

    bool setup(Runtime::Execution::PipelineExecutionContextPtr) override { return false; }

    Windowing::WindowManagerPtr getWindowManager() override { return NES::Windowing::WindowManagerPtr(); }

    std::shared_ptr<Windowing::MultiOriginWatermarkProcessor> getWatermarkProcessor() { return watermarkProcessor; }

    std::string toString() override { return std::string(); }

    std::array<SliceStore, 10> threadLocalSliceStore;
    SliceStore globalSliceStore;
};

class RobinThreadLocalPreAggregateWindowOperator : public Runtime::Execution::ExecutablePipelineStage {

  public:
    RobinThreadLocalPreAggregateWindowOperator(Windowing::LogicalWindowDefinitionPtr windowDefinition)
        : windowDefinition(windowDefinition){};
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
    Windowing::WindowOperatorHandlerPtr windowOperatorHandler;
    struct Record {
        uint64_t id;
        uint64_t value;
        uint64_t payload;
        uint64_t timestamp;
    };
    uint32_t setup(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override {
        {
            windowOperatorHandler = Windowing::WindowOperatorHandler::create(windowDefinition, SchemaPtr());

            // calculate the number of slices
            if (windowDefinition->getWindowType()->isTumblingWindow()) {
                auto tumblingWindow = dynamic_pointer_cast<Windowing::TumblingWindow>(windowDefinition->getWindowType());
                auto numberOfSlices = windowDefinition->getAllowedLateness() / tumblingWindow->getSize().getTime();
            } else {
                NES_THROW_RUNTIME_ERROR("Only Tumbling windows are supported");
            }

            auto windowHandler = std::make_shared<ThreadLocalWindowHandler<int64_t, int64_t, int64_t, int64_t>>(windowDefinition);
            windowOperatorHandler->setWindowHandler(windowHandler);
            windowHandler->setup(pipelineExecutionContext.shared_from_this());
        };
        return 0;
    }
    __attribute__((noinline)) ExecutionResult execute(Runtime::TupleBuffer& inputTupleBuffer,
                                                      Runtime::Execution::PipelineExecutionContext&,
                                                      Runtime::WorkerContext& workerContext) override {

        auto inputTuples = inputTupleBuffer.getBuffer<Record>();

        auto windowHandler =
            windowOperatorHandler->getWindowHandler<ThreadLocalWindowHandler, uint64_t, uint64_t, uint64_t, uint64_t>();

        auto windowManager = windowHandler->getWindowManager();
        auto allowedLateness = 0;
        auto workerId = workerContext.getId();
        auto threadLocalSliceStore = windowHandler->threadLocalSliceStore[workerId];
        auto records = inputTupleBuffer.getNumberOfTuples();
        uint64_t currentWatermark = 0;
        for (uint64_t recordIndex = 0; recordIndex < records; ++recordIndex) {
            uint64_t key = inputTuples[recordIndex].id;
            auto current_ts = inputTuples[recordIndex].timestamp;

            currentWatermark = std::max(currentWatermark, current_ts - allowedLateness);
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
                // the global watermark was changed, so we can merge all slides, which
                // ended before the watermark. As the watermark passed the slice end,
                // there can't be any concurrent updates to this slide.
                // The global slice store still contains slices, should we maybe directly output windows here?
                // todo dispatch the merging for each local store to an own task
                // This iteration is safe, because the number of thread local slice stores is fixed at compilation time
                for (auto const& localLiceStore : windowHandler->threadLocalSliceStore) {
                    // iterate about all slices in this slice store
                    // todo maybe we can infer the index directly and omit the
                    // looping/check?
                    for (auto const& slice : localLiceStore.slices) {
                        if (slice.end >= newWatermark) {
                            // merge key space in global map
                            auto globalSlice = windowHandler->globalSliceStore.findSliceByTs(slice.start);
                            for (auto const& [key, value] : slice.map) {
                                globalSlice.map[key] = globalSlice.map[key] + value;
                            }
                        }
                    }
                }

                // check if windows should be triggered
                // todo place in own task
                auto windows = std::vector<Windowing::WindowState>();
                windowHandler->getWindowDefinition()->getWindowType()->triggerWindows(windows,
                                                                                      windowHandler->getLastWatermark(),
                                                                                      currentWatermark);
                for (auto const& window : windows) {
                    // todo maybe we can infer the index directly and omit the
                    // create window aggregates, this is then the global hash-map and
                    // requires merging of all keys
                    robin_hood::unordered_flat_map<uint64_t, uint64_t> windowAggregates;
                    for (auto const& slice : windowHandler->globalSliceStore.slices) {
                        if (slice.start >= window.getStartTs() && slice.end <= window.getEndTs()) {
                            // append to window aggregate
                            for (auto const& [key, value] : slice.map) {
                                windowAggregates[key] = windowAggregates[key] + value;
                            }
                        }
                    }
                    // emit window results
                    auto resultBuffer = workerContext.allocateTupleBuffer();
                    auto resultTuples = resultBuffer.getBuffer<WindowResultRecord>();
                    for (auto const& [key, value] : windowAggregates) {
                        resultTuples->windowStart = window.getStartTs();
                        resultTuples->windowEnd = window.getEndTs();
                        resultTuples->key = key;
                        resultTuples->aggregateValue = value;
                    }
                }
            }
        }
        return ExecutionResult::Ok;
    }
};
}// namespace NES::Experimental

#endif// NES_BENCHMARK_INCLUDE_MICROBENCHMARKS_WINDOWING_ROBBINTHREADLOCALPREAGGREGATEWINDOWOPERATOR_HPP_
