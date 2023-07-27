

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_ABSTRACTSLICEPREAGGREGATIONHANDLER_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_ABSTRACTSLICEPREAGGREGATIONHANDLER_HPP_
#include <Common/Identifiers.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>
#include <vector>

namespace NES::Runtime::Execution::Operators {
class MultiOriginWatermarkProcessor;

/**
 * @brief The AbstractSlicePreAggregationHandler provides an abstract operator handler to perform slice-based pre-aggregation
 * of time-based tumbling and sliding windows.
 * This operator handler, maintains a slice store for each worker thread and provides them for the aggregation.
 * For each processed tuple buffer trigger is called, which checks if the thread-local slice store should be triggered.
 * This is decided by the current watermark timestamp.
 */
template<class SliceStaging, typename SliceStore>
class AbstractSlicePreAggregationHandler : public Runtime::Execution::OperatorHandler {
  public:
    /**
     * @brief Creates the operator handler with a specific window definition, a set of origins, and access to the slice staging object.
     * @param windowDefinition logical window definition
     * @param origins the set of origins, which can produce data for the window operator
     * @param weakSliceStagingPtr access to the slice staging.
     */
    AbstractSlicePreAggregationHandler(uint64_t windowSize,
                                       uint64_t windowSlide,
                                       const std::vector<OriginId>& origins,
                                       std::weak_ptr<SliceStaging> weakSliceStagingPtr)
        : windowSize(windowSize), windowSlide(windowSlide), weakSliceStaging(weakSliceStagingPtr),
          watermarkProcessor(std::make_unique<MultiOriginWatermarkProcessor>(origins)){};

    SliceStore* getThreadLocalSliceStore(uint64_t workerId) {
        auto index = workerId % threadLocalSliceStores.size();
        return threadLocalSliceStores[index].get();
    }

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
    void trigger(Runtime::WorkerContext& wctx,
                 Runtime::Execution::PipelineExecutionContext& ctx,
                 OriginId originId,
                 uint64_t sequenceNumber,
                 uint64_t watermarkTs) {
        // the watermark update is an atomic process and returns the last and the current watermark.
        NES_DEBUG("{} Trigger {}-{}-{}", windowSize, originId, sequenceNumber, watermarkTs);
        auto currentWatermark = watermarkProcessor->updateWatermark(watermarkTs, sequenceNumber, originId);
        // todo try cas
        if (lastTriggerWatermark == currentWatermark) {
            // if the current watermark has not changed, we don't have to trigger any windows and return.
            return;
        }

        // get the lock to trigger
        std::lock_guard<std::mutex> lock(triggerMutex);
        // update currentWatermark, such that other threads to have to acquire the lock
        auto oldWatermark = lastTriggerWatermark.load();
        lastTriggerWatermark = currentWatermark;

        NES_DEBUG("{} Trigger slices between {}-{}", windowSize, oldWatermark, lastTriggerWatermark);
        auto sliceStaging = this->weakSliceStaging.lock();
        if (!sliceStaging) {
            NES_FATAL_ERROR("SliceStaging is invalid, this should only happen after a hard stop. Drop all in flight data.");
            return;
        }

        std::set<std::tuple<uint64_t, uint64_t>> sliceMetaData;
        for (auto& threadLocalSliceStore : threadLocalSliceStores) {
            auto slices = threadLocalSliceStore->extractSlicesUntilTs(lastTriggerWatermark);
            for (auto& slice : slices) {
                sliceMetaData.emplace(slice->getStart(), slice->getEnd());
                NES_TRACE("Assign thread local slices {}-{}",
                          slice->getStart(),
                          slice->getEnd());
                sliceStaging->addToSlice(slice->getEnd(), std::move(slice->getState()));
            }
            threadLocalSliceStore->setLastWatermark(lastTriggerWatermark);
        }

        for (const auto& [sliceStart, sliceEnd] : sliceMetaData) {
            NES_DEBUG("{} Deploy merge task for slice {}-{} ", windowSize, sliceStart, sliceEnd);
            auto buffer = wctx.allocateTupleBuffer();
            auto task = buffer.getBuffer<SliceMergeTask>();
            task->startSlice = sliceStart;
            task->endSlice = sliceEnd;
            task->sequenceNumber = resultSequenceNumber++;
            buffer.setNumberOfTuples(1);
            ctx.dispatchBuffer(buffer);
        }
    };

    void start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {
        NES_DEBUG("start GlobalSlicePreAggregationHandler");
    }

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) {
        NES_DEBUG("shutdown GlobalSlicePreAggregationHandler: {}", queryTerminationType);

        // get the lock to trigger -> this should actually not be necessary, as stop can not be called concurrently to the processing.
        std::lock_guard<std::mutex> lock(triggerMutex);

        if (queryTerminationType == Runtime::QueryTerminationType::Graceful) {
            auto sliceStaging = this->weakSliceStaging.lock();
            NES_ASSERT(sliceStaging, "SliceStaging is invalid, this should only happen after a soft stop.");

            std::set<std::tuple<uint64_t, uint64_t>> sliceMetaData;

            for (auto& threadLocalSliceStore : threadLocalSliceStores) {
                // we can directly access the slices as no other worker can concurrently change them
                for (auto& slice : threadLocalSliceStore->getSlices()) {
                    auto& sliceState = slice->getState();
                    // each worker adds its local state to the staging area
                    sliceStaging->addToSlice(slice->getEnd(), std::move(sliceState));
                    sliceMetaData.emplace(slice->getStart(), slice->getEnd());
                }
            }

            for (const auto& [sliceStart, sliceEnd] : sliceMetaData) {
                NES_DEBUG("{} Deploy merge task for slice {}-{} ", windowSize, sliceStart, sliceEnd);
                auto buffer = pipelineExecutionContext->getBufferManager()->getBufferBlocking();
                auto task = buffer.getBuffer<SliceMergeTask>();
                task->startSlice = sliceStart;
                task->endSlice = sliceEnd;
                task->sequenceNumber = resultSequenceNumber++;
                buffer.setNumberOfTuples(1);
                pipelineExecutionContext->dispatchBuffer(buffer);
            }
        }
    }

  protected:
    const uint64_t windowSize;
    const uint64_t windowSlide;
    std::weak_ptr<SliceStaging> weakSliceStaging;
    std::vector<std::unique_ptr<SliceStore>> threadLocalSliceStores;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
    std::atomic<uint64_t> lastTriggerWatermark = 0;
    std::atomic<uint64_t> resultSequenceNumber = 1;
    std::mutex triggerMutex;
};
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_ABSTRACTSLICEPREAGGREGATIONHANDLER_HPP_
