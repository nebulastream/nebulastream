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
#include <Windowing/Watermark/MultiOriginWatermarkProcessor.hpp>
#include <Windowing/Watermark/WatermarkProcessor.hpp>
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
#include <atomic>
namespace NES::Experimental {


/**
 * @brief Implements a log free watermark processor for a single origin.
 * It processes all watermark updates from one specific origin and applies all updates in sequential order.
 * @assumptions This watermark processor assumes strictly monotonic update sequence numbers.
 * @assumption This watermark processor maintains a fixed size log of inflight watermark updates.
 * #TODO add spin lock if the log is full, currently we just fail.
 *
 * Implementation details:
 * This watermark processor stores in flight updates in a fixed size log, implemented with a cycling buffer.
 * We differentiate two cases:
 *
 * 1. If the sequenceNumber of the watermark updates is the next expected sequence number, i.e., currentWatermarkTuple.sequenceNumber + 1.
 * In this case we can update the currentWatermarkTuple as we received the next update.
 * To this end, we first iterate over the log and apply all updates as log as the sequence number is as expected.
 *
 * 2. If the sequenceNumber is different then the next expected sequence number.
 * In this case, we can't update the currentWatermarkTuple.
 * Instead, we store the update in the correct log position.
 *
 * @tparam logSize
 */
template<uint64_t logSize = 100>
class LookFreeWatermarkProcessor {

  public:
    WatermarkTs updateWatermark(WatermarkTs ts, SequenceNumber sequenceNumber) {
        // A new sequence number has to be greater than the currentWatermarkTuple.sequenceNumber.
        assert(sequenceNumber > currentWatermarkTuple.sequenceNumber);

        // We currently assume that the diff between sequence number and current sequence number will not exceed the log size.
        // Otherwise, we would override content in the log
        assert(sequenceNumber - currentWatermarkTuple.sequenceNumber < logSize);

        // Check if we got the next valid sequence number.
        // Only a single thread can have a matching sequence number, thus we know that no other thread can access the true case,
        // till we update the currentWatermarkTuple.sequenceNumber
        if (sequenceNumber == currentWatermarkTuple.load().sequenceNumber + 1) {
            WatermarkTuple nextWatermarkTuple = {ts, sequenceNumber};
            // We first process the log to check if we can actually can apply multiple updates in one step.
            // We apply updates from the log as long as the value contains the next expected sequence number.
            while(readWatermarkTuple(nextWatermarkTuple.sequenceNumber + 1).sequenceNumber == nextWatermarkTuple.sequenceNumber + 1){
                nextWatermarkTuple = readWatermarkTuple(sequenceNumber + 1);
            };
            // Set the watermark ts and sequence number atomically
            currentWatermarkTuple = nextWatermarkTuple;
        } else {
            // The sequence number is not the next expected.
            // So we store it in the correct lock position.
            auto logIndex = getLogIndex(sequenceNumber);
            log[logIndex] = {ts, sequenceNumber};
        }
    }

  private:
    struct WatermarkTuple {
        WatermarkTs ts;
        SequenceNumber sequenceNumber;
    };

    WatermarkTuple& readWatermarkTuple(SequenceNumber sequenceNumber) { return log[getLogIndex(sequenceNumber)].load(); }

    uint64_t getLogIndex(SequenceNumber sequenceNumber) { return sequenceNumber % logSize; }

    std::array<std::atomic<WatermarkTuple>, logSize> log;
    std::atomic<WatermarkTuple> currentWatermarkTuple;
    static_assert(std::atomic<WatermarkTuple>::is_always_lock_free());
};

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
    //robin_hood::unordered_flat_map<uint64_t, uint64_t> map;
};

// A slice store, which contains a set of slices
class SliceStore {
  public:
    explicit SliceStore(uint64_t sliceSize) : sliceSize(sliceSize){};
    inline KeyedSlice& findSliceByTs(uint64_t ts) {

        // create the first slice if non is existing [0 - slice size]
        if (slices.empty()) {
            slices.emplace_back(0, sliceSize);
        }

        // if the last slice ends before the current event ts we have to add new slices to the end as long es we don't cover ts.
        // if the gap between last and ts is large this could create a large number of slices.
        while (last().end <= ts) {
            slices.emplace_back(last().end, last().end + sliceSize);
        }

        // find the correct slice
        // calculate the slice offset by the start ts and the event ts.
        auto diffFromStart = ts - first().start;
        auto index = diffFromStart / sliceSize;
        return slices[index];
    }
    inline KeyedSlice& first() { return slices[0]; }
    inline KeyedSlice& last() { return slices[slices.size() - 1]; }
    std::vector<KeyedSlice>& getSlices() { return slices; }
    uint64_t lastThreadLocalWatermark = 0;
    uint64_t outputs = 0;

  private:
    uint64_t sliceSize;
    std::vector<KeyedSlice> slices{};
};

class GlobalSliceStore : public SliceStore {
  public:
    GlobalSliceStore(uint64_t sliceSize) : SliceStore(sliceSize){};
    std::mutex mutex;
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
    ThreadLocalWindowHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                             uint64_t sliceSize,
                             uint64_t numberOfThreads)
        : AbstractWindowHandler(windowDefinition), sliceSize(sliceSize), globalSliceStore(sliceSize) {

        for (uint64_t i = 0; i < numberOfThreads; i++) {
            threadLocalSliceStore.emplace_back(sliceSize);
        }
        threadWatermarkProcessor = std::make_shared<Windowing::MultiOriginWatermarkProcessor>(numberOfThreads);
    }
    bool start(Runtime::StateManagerPtr, uint32_t) override { return true; }
    bool stop() override { return true; }

    void trigger(Runtime::WorkerContext&, bool) override {}

    bool setup(Runtime::Execution::PipelineExecutionContextPtr) override { return false; }

    Windowing::WindowManagerPtr getWindowManager() override { return NES::Windowing::WindowManagerPtr(); }

    std::shared_ptr<Windowing::MultiOriginWatermarkProcessor> getWatermarkProcessor() { return watermarkProcessor; }

    std::string toString() override { return std::string(); }

    const uint64_t sliceSize;
    std::vector<SliceStore> threadLocalSliceStore;
    GlobalSliceStore globalSliceStore;
    std::shared_ptr<Windowing::MultiOriginWatermarkProcessor> threadWatermarkProcessor;
};

class RobinThreadLocalPreAggregateWindowOperator : public Runtime::Execution::ExecutablePipelineStage {

  public:
    RobinThreadLocalPreAggregateWindowOperator(Windowing::LogicalWindowDefinitionPtr windowDefinition)
        : windowDefinition(windowDefinition){};
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
    Windowing::WindowOperatorHandlerPtr windowOperatorHandler;
    struct Record {
        uint64_t test$key;
        uint64_t test$value;
        uint64_t payload;
        uint64_t test$ts;
    };
    uint32_t setup(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override {
        {
            windowOperatorHandler = Windowing::WindowOperatorHandler::create(windowDefinition, SchemaPtr());

            // calculate the number of slices
            uint64_t sliceSize = 0;
            if (windowDefinition->getWindowType()->isTumblingWindow()) {
                auto tumblingWindow = dynamic_pointer_cast<Windowing::TumblingWindow>(windowDefinition->getWindowType());
                // auto numberOfSlices = windowDefinition->getAllowedLateness() / tumblingWindow->getSize().getTime();
                sliceSize = tumblingWindow->getSize().getTime();
            } else {
                auto slidingWindow = dynamic_pointer_cast<Windowing::SlidingWindow>(windowDefinition->getWindowType());
                // auto numberOfSlices = windowDefinition->getAllowedLateness() / tumblingWindow->getSize().getTime();
                sliceSize = slidingWindow->getSlide().getTime();
            }
            auto numberOfWorkerThreads = pipelineExecutionContext.getNumberOfWorkerThreads();
            auto windowHandler =
                std::make_shared<ThreadLocalWindowHandler<int64_t, int64_t, int64_t, int64_t>>(windowDefinition,
                                                                                               sliceSize,
                                                                                               numberOfWorkerThreads);
            windowOperatorHandler->setWindowHandler(windowHandler);
            windowHandler->setup(pipelineExecutionContext.shared_from_this());
        };
        return 0;
    }
    uint32_t open(Runtime::Execution::PipelineExecutionContext&, Runtime::WorkerContext&) override { return 0; }
    __attribute__((noinline)) ExecutionResult execute(Runtime::TupleBuffer& inputTupleBuffer,
                                                      Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
                                                      Runtime::WorkerContext& workerContext) override {

        auto inputTuples = inputTupleBuffer.getBuffer<Record>();

        auto windowHandler =
            windowOperatorHandler->getWindowHandler<ThreadLocalWindowHandler, uint64_t, uint64_t, uint64_t, uint64_t>();

        auto windowManager = windowHandler->getWindowManager();
        auto allowedLateness = 0;
        auto workerId = workerContext.getId();
        auto& threadLocalSliceStore = windowHandler->threadLocalSliceStore[workerId];
        auto records = inputTupleBuffer.getNumberOfTuples();
        uint64_t currentWatermark = 0;
        for (uint64_t recordIndex = 0; recordIndex < records; ++recordIndex) {
            uint64_t key = inputTuples[recordIndex].test$key;
            auto current_ts = inputTuples[recordIndex].test$ts;

            currentWatermark = std::max(currentWatermark, current_ts - allowedLateness);
            // pre aggregate in local slice store
            auto& slice = threadLocalSliceStore.findSliceByTs(current_ts);

            slice.map[key] = slice.map[key] + inputTuples[recordIndex].test$value;
        };
        // We now have to check if the current watermark shifts the global watermark
        // Check if the current watermark is part of a later slice then the last watermark.
        auto lastLocalWatermark = threadLocalSliceStore.lastThreadLocalWatermark;
        if (currentWatermark / windowHandler->sliceSize > lastLocalWatermark / windowHandler->sliceSize) {
            // update the global watermark, and merge all local slides.
            auto watermarkProcessor = windowHandler->getWatermarkProcessor();
            // the watermark update is an atomic process and returns the last and the current watermark.
            auto [_, newGlobalWatermark] = watermarkProcessor->updateWatermark(currentWatermark,
                                                                               inputTupleBuffer.getSequenceNumber(),
                                                                               inputTupleBuffer.getOriginId());
            // check if the current max watermark is larger than the thread local watermark
            if (threadLocalSliceStore.lastThreadLocalWatermark < newGlobalWatermark) {
                // push the local slices to the global slice store.
                for (auto const& slice : threadLocalSliceStore.getSlices()) {
                    if (slice.end >= newGlobalWatermark) {
                        break;
                    }
                    auto lock = std::unique_lock(windowHandler->globalSliceStore.mutex);
                    // merge key space in global map
                    auto& globalSlice = windowHandler->globalSliceStore.findSliceByTs(slice.start);
                    for (auto const& [key, value] : slice.map) {
                        globalSlice.map[key] = globalSlice.map[key] + value;
                    }
                }
                threadLocalSliceStore.lastThreadLocalWatermark = newGlobalWatermark;
                threadLocalSliceStore.outputs++;
                // update the watermark to check if all threads already triggered this slice
                auto [oldCrossThreadWatermark, newCrossThreadWatermark] =
                    windowHandler->threadWatermarkProcessor->updateWatermark(newGlobalWatermark,
                                                                             threadLocalSliceStore.outputs,
                                                                             workerId);
                if (oldCrossThreadWatermark != newCrossThreadWatermark) {
                    // the global watermark changed and all threads put their local slices to the global state.
                    // so we can trigger all windows between the watermarks.
                    auto windows = std::vector<Windowing::WindowState>();
                    windowHandler->getWindowDefinition()->getWindowType()->triggerWindows(windows,
                                                                                          oldCrossThreadWatermark,
                                                                                          newCrossThreadWatermark);
                    if (!windows.empty()) {
                        // lock the global slice store
                        auto lock = std::unique_lock(windowHandler->globalSliceStore.mutex);
                        auto resultBuffer = workerContext.allocateTupleBuffer();
                        auto resultTuples = resultBuffer.getBuffer<WindowResultRecord>();
                        for (auto const& window : windows) {
                            // todo maybe we can infer the index directly and omit the
                            // create window aggregates, this is then the global hash-map and
                            // requires merging of all keys
                            robin_hood::unordered_flat_map<uint64_t, uint64_t> windowAggregates;
                            for (auto const& slice : windowHandler->globalSliceStore.getSlices()) {
                                if (slice.start >= window.getStartTs() && slice.end <= window.getEndTs()) {
                                    // append to window aggregate
                                    for (auto const& [key, value] : slice.map) {
                                        windowAggregates[key] = windowAggregates[key] + value;
                                    }
                                }
                                if (slice.end > window.getEndTs()) {
                                    break;
                                }
                            }
                            // emit window results

                            for (auto const& [key, value] : windowAggregates) {
                                auto index = resultBuffer.getNumberOfTuples();
                                resultTuples[index].windowStart = window.getStartTs();
                                resultTuples[index].windowEnd = window.getEndTs();
                                resultTuples[index].key = key;
                                resultTuples[index].aggregateValue = value;
                                resultBuffer.setNumberOfTuples(index + 1);
                            }
                        }
                        if (resultBuffer.getNumberOfTuples() != 0)
                            pipelineExecutionContext.emitBuffer(resultBuffer, workerContext);
                    }
                }
            }
        }
        return ExecutionResult::Ok;
    }
};
}// namespace NES::Experimental

#endif// NES_BENCHMARK_INCLUDE_MICROBENCHMARKS_WINDOWING_ROBBINTHREADLOCALPREAGGREGATEWINDOWOPERATOR_HPP_
