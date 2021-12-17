/*
Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Windowing/Experimental/LockFreeWatermarkProcessor.hpp>
#include <Windowing/Experimental/SliceStore.hpp>
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
 * @brief The ResultRecord of a window aggregation
 */
struct WindowResultRecord {
    uint64_t windowStart;
    uint64_t windowEnd;
    uint64_t key;
    uint64_t aggregateValue;
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

        std::vector<uint64_t> originIds;
        for (uint64_t i = 0; i < windowDefinition->getNumberOfInputEdges(); i++) {
            originIds.emplace_back(i);
        }
        threadWatermarkProcessor = std::make_shared<Windowing::MultiOriginWatermarkProcessor>(numberOfThreads);
        lockFreeWatermarkProcessor = std::make_shared<LockFreeMultiOriginWatermarkProcessor>(originIds);
    }
    bool start(Runtime::StateManagerPtr, uint32_t) override { return true; }
    bool stop() override { return true; }

    void trigger(Runtime::WorkerContext&, bool) override {}

    bool setup(Runtime::Execution::PipelineExecutionContextPtr) override { return false; }

    Windowing::WindowManagerPtr getWindowManager() override { return NES::Windowing::WindowManagerPtr(); }

    std::shared_ptr<LockFreeMultiOriginWatermarkProcessor> getWatermarkProcessor() { return lockFreeWatermarkProcessor; }

    std::string toString() override { return std::string(); }

    const uint64_t sliceSize;
    std::vector<SliceStore<robin_hood::unordered_flat_map<uint64_t, uint64_t>>> threadLocalSliceStore;
    GlobalSliceStore<robin_hood::unordered_flat_map<uint64_t, uint64_t>> globalSliceStore;
    std::shared_ptr<Windowing::MultiOriginWatermarkProcessor> threadWatermarkProcessor;
    std::shared_ptr<LockFreeMultiOriginWatermarkProcessor> lockFreeWatermarkProcessor;
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

        //  auto windowManager = windowHandler->getWindowManager();
        auto allowedLateness = 0;
        auto workerId = (workerContext.getId()) % pipelineExecutionContext.getNumberOfWorkerThreads();
        auto& threadLocalSliceStore = windowHandler->threadLocalSliceStore[workerId];
        auto records = inputTupleBuffer.getNumberOfTuples();
        uint64_t currentWatermark = 0;
        for (uint64_t recordIndex = 0; recordIndex < records; ++recordIndex) {
            uint64_t key = inputTuples[recordIndex].test$key;
            auto current_ts = inputTuples[recordIndex].test$ts;

            currentWatermark = std::max(currentWatermark, current_ts - allowedLateness);
            // pre aggregate in local slice store
            auto& slice = threadLocalSliceStore.findSliceByTs(current_ts);

            slice->map[key] = slice->map[key] + inputTuples[recordIndex].test$value;
        };
        // only increase watermark in 10000 tuples.
        //currentWatermark = currentWatermark - (currentWatermark % 10000);
        // We now have to check if the current watermark shifts the global watermark
        // Check if the current watermark is part of a later slice then the last watermark.
        // update the global watermark, and merge all local slides.
        auto watermarkProcessor = windowHandler->getWatermarkProcessor();
        // the watermark update is an atomic process and returns the last and the current watermark.
        auto newGlobalWatermark = watermarkProcessor->updateWatermark(currentWatermark,
                                                                      inputTupleBuffer.getSequenceNumber(),
                                                                      inputTupleBuffer.getOriginId());
        // check if the current max watermark is larger than the thread local watermark
        if (threadLocalSliceStore.lastThreadLocalWatermark < newGlobalWatermark) {
            //std::cout << "update watermark at " << newGlobalWatermark << std::endl;
            // push the local slices to the global slice store.
            for (uint64_t sliceIndex = threadLocalSliceStore.getFirstIndex(); sliceIndex != threadLocalSliceStore.getLastIndex();
                 sliceIndex++) {
                auto& slice = threadLocalSliceStore[sliceIndex];
                if (slice->end >= newGlobalWatermark) {
                    break;
                }
                //   std::cout << " Merge slice (" << slice->start << "-" << slice->end << ") to global" << std::endl;
                auto lock = std::unique_lock(windowHandler->globalSliceStore.mutex);
                // merge key space in global map
                auto& globalSlice = windowHandler->globalSliceStore.findSliceByTs(slice->start);
                for (auto const& [key, value] : slice->map) {
                    globalSlice->map[key] = globalSlice->map[key] + value;
                }
                threadLocalSliceStore.eraseFirst(1);
            }
            // drop local slices
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
                    auto bufferCapacity = (resultBuffer.getBufferSize() / sizeof(WindowResultRecord));
                    uint64_t minWindowStart = 0;
                    for (auto const& window : windows) {
                        minWindowStart = std::max(minWindowStart, window.getStartTs());
                        // todo maybe we can infer the index directly and omit the
                        // create window aggregates, this is then the global hash-map and
                        // requires merging of all keys
                        robin_hood::unordered_flat_map<uint64_t, uint64_t> windowAggregates;
                        auto& globalSlices = windowHandler->globalSliceStore.getSlices();
                        for (uint64_t sliceIndex = windowHandler->globalSliceStore.getFirstIndex();
                             sliceIndex < windowHandler->globalSliceStore.getLastIndex();
                             sliceIndex++) {
                            auto& slice = windowHandler->globalSliceStore[sliceIndex];
                            if (slice->start >= window.getStartTs() && slice->end <= window.getEndTs()) {
                                // append to window aggregate
                                for (auto const& [key, value] : slice->map) {
                                    windowAggregates[key] = windowAggregates[key] + value;
                                }
                            }
                            if (slice->end > window.getEndTs()) {
                                break;
                            }
                        }
                        // emit window results
                        //  std::cout << " Emit Window (" << window.getStartTs() << "-" << window.getEndTs() << ")" << std::endl;
                        for (auto const& [key, value] : windowAggregates) {
                            auto index = resultBuffer.getNumberOfTuples();
                            resultTuples[index].windowStart = window.getStartTs();
                            resultTuples[index].windowEnd = window.getEndTs();
                            resultTuples[index].key = key;
                            resultTuples[index].aggregateValue = value;
                            resultBuffer.setNumberOfTuples(index + 1);
                            // early emitt if buffer is full
                            if (resultBuffer.getNumberOfTuples() == bufferCapacity) {
                                pipelineExecutionContext.emitBuffer(resultBuffer, workerContext);
                                resultBuffer = workerContext.allocateTupleBuffer();
                            }
                        }
                    }
                    if (resultBuffer.getNumberOfTuples() != 0) {
                        pipelineExecutionContext.emitBuffer(resultBuffer, workerContext);
                    }
                    auto& gloabalSlices = windowHandler->globalSliceStore.getSlices();
                    for (uint64_t sliceIndex = windowHandler->globalSliceStore.getFirstIndex();
                         sliceIndex < windowHandler->globalSliceStore.getLastIndex();
                         sliceIndex++) {
                        if (windowHandler->globalSliceStore[sliceIndex]->end < newGlobalWatermark) {
                            windowHandler->globalSliceStore.eraseFirst(1);
                        }
                    }
                }
            }
        }
        return ExecutionResult::Ok;
    }
};
}// namespace NES::Experimental

#endif// NES_BENCHMARK_INCLUDE_MICROBENCHMARKS_WINDOWING_ROBBINTHREADLOCALPREAGGREGATEWINDOWOPERATOR_HPP_
