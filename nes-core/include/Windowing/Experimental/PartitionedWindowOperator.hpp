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

#ifndef NES_BENCHMARK_INCLUDE_MICROBENCHMARKS_WINDOWING_PARTITIONEDPREAGGREGATEWINDOWOPERATOR_HPP_
#define NES_BENCHMARK_INCLUDE_MICROBENCHMARKS_WINDOWING_PARTITIONEDPREAGGREGATEWINDOWOPERATOR_HPP_

#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <State/StateVariable.hpp>
#include <Util/Logger.hpp>
#include <Windowing/Experimental/LockFreeWatermarkProcessor.hpp>
#include <Windowing/Experimental/SeqenceLog.hpp>
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
#include <string>
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

class PartitionMergeTask {
  public:
    uint64_t partitionIndex;
    uint64_t sliceIndex;
    ~PartitionMergeTask(){
        //std::cout << "~PartitionMergeTask" << std::endl;
    };
};

struct WindowAggregateTask {
    uint64_t partitionIndex;
    uint64_t startSliceIndex;
    uint64_t endSliceIndex;
    uint64_t triggerSequenceNumber;
};

template<class KeyType, class InputType>
class PreAggregationWindowHandler
    : public detail::virtual_enable_shared_from_this<PreAggregationWindowHandler<KeyType, InputType>, false>,
      public Runtime::Reconfigurable {
    using inherited0 = detail::virtual_enable_shared_from_this<PreAggregationWindowHandler, false>;
    using inherited1 = Runtime::Reconfigurable;

  public:
    PreAggregationWindowHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                Runtime::BufferManagerPtr bufferManager,
                                uint64_t sliceSize,
                                uint64_t numberOfThreads)
        : sliceSize(sliceSize), globalSliceStore(bufferManager), bufferManager(bufferManager) {
        std::vector<uint64_t> originIds;
        for (uint64_t i = 0; i < windowDefinition->getNumberOfInputEdges(); i++) {
            originIds.emplace_back(i + 1);
        }
        lockFreeWatermarkProcessor = std::make_shared<LockFreeWatermarkProcessor<>>();
    }

    void setup(Runtime::Execution::PipelineExecutionContext& ctx) {
        for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++) {
            threadLocalSliceStore.emplace_back(bufferManager, sliceSize);
        }
    }
    const uint64_t sliceSize;
    std::vector<PartitionedSliceStore<uint64_t, uint64_t>> threadLocalSliceStore;
    PreAggregateSliceStaging<uint64_t, uint64_t> preAggregateSliceStaging;
    GlobalAggregateStore<uint64_t, uint64_t> globalSliceStore;
    std::shared_ptr<LockFreeWatermarkProcessor<>> lockFreeWatermarkProcessor;
    Runtime::BufferManagerPtr bufferManager;
    std::mutex mutex;
};

class PreAggregateWindowOperator : public Runtime::Execution::ExecutablePipelineStage {

  public:
    PreAggregateWindowOperator(std::shared_ptr<PreAggregationWindowHandler<uint64_t, uint64_t>> windowOperatorHandler)
        : windowOperatorHandler(windowOperatorHandler){};
    std::shared_ptr<PreAggregationWindowHandler<uint64_t, uint64_t>> windowOperatorHandler;

    struct Record {
        uint64_t test$key;
        uint64_t test$value;
        uint64_t payload;
        uint64_t test$ts;
    };

    uint32_t setup(Runtime::Execution::PipelineExecutionContext& ctx) override {
        windowOperatorHandler->setup(ctx);
        return 0;
    }

    uint32_t open(Runtime::Execution::PipelineExecutionContext&, Runtime::WorkerContext&) override { return 0; }
    __attribute__((noinline)) ExecutionResult execute(Runtime::TupleBuffer& inputTupleBuffer,
                                                      Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
                                                      Runtime::WorkerContext& workerContext) override {

        auto inputTuples = inputTupleBuffer.getBuffer<Record>();

        //  auto windowManager = windowHandler->getWindowManager();
        auto allowedLateness = 0;
        auto workerId = (workerContext.getId()) % pipelineExecutionContext.getNumberOfWorkerThreads();
        auto& threadLocalSliceStore = windowOperatorHandler->threadLocalSliceStore[workerId];
        auto records = inputTupleBuffer.getNumberOfTuples();
        uint64_t currentWatermark = 0;
        for (uint64_t recordIndex = 0; recordIndex < records; ++recordIndex) {
            uint64_t key = inputTuples[recordIndex].test$key;
            auto current_ts = inputTuples[recordIndex].test$ts;
            if (current_ts < threadLocalSliceStore.lastThreadLocalWatermark) {
                continue;
            }
            currentWatermark = std::max(currentWatermark, current_ts - allowedLateness);
            // pre aggregate in local slice store
            auto& slice = threadLocalSliceStore.findSliceByTs(current_ts);
            auto* entry = slice->map.getEntry(key);
            entry->value = entry->value + inputTuples[recordIndex].test$value;
        };

        auto watermarkProcessor = windowOperatorHandler->lockFreeWatermarkProcessor;
        // the watermark update is an atomic process and returns the last and the current watermark.
        auto newGlobalWatermark = watermarkProcessor->updateWatermark(currentWatermark, inputTupleBuffer.getSequenceNumber());
        // check if the current max watermark is larger than the thread local watermark
        if (threadLocalSliceStore.lastThreadLocalWatermark < newGlobalWatermark) {
            //std::cout << "update watermark at " << newGlobalWatermark << std::endl;
            // push the local slices to the global slice store.
            for (uint64_t si = threadLocalSliceStore.getFirstIndex(); si != threadLocalSliceStore.getLastIndex(); si++) {
                auto& slice = threadLocalSliceStore[si];
                if (slice->end >= newGlobalWatermark) {
                    break;
                }

                // put partitions to global slice store
                auto& sliceState = slice->map;

                for (uint64_t partitionIndex = 0; partitionIndex < sliceState.getNumberOfPartitions(); partitionIndex++) {
                    auto& preAggregateSliceStaging = windowOperatorHandler->preAggregateSliceStaging;
                    auto localPartition = std::move(sliceState.getPartition(partitionIndex));
                    auto addedPartitionsToSlice =
                        preAggregateSliceStaging.addPartition(slice->sliceIndex, std::move(localPartition));
                    if (addedPartitionsToSlice == pipelineExecutionContext.getNumberOfWorkerThreads()) {
                        auto buffer = workerContext.allocateTupleBuffer();
                        auto task = buffer.getBuffer<PartitionMergeTask>();
                        task->sliceIndex = slice->sliceIndex;
                        task->partitionIndex = partitionIndex;
                        buffer.setNumberOfTuples(1);
                        pipelineExecutionContext.dispatchBuffer(buffer);
                    }
                }
                // erase slice from local slice store
                threadLocalSliceStore.eraseFirst(1);
            }
            threadLocalSliceStore.lastThreadLocalWatermark = newGlobalWatermark;
        }
        return ExecutionResult::Ok;
    }

    virtual uint32_t stop(Runtime::Execution::PipelineExecutionContext&) {
        std::cout << " stop PreAggregateWindowOperator" << std::endl;
        return 0;
    }
};

class MergeSliceWindowOperator : public Runtime::Execution::ExecutablePipelineStage {
  public:
    MergeSliceWindowOperator(std::shared_ptr<PreAggregationWindowHandler<uint64_t, uint64_t>> windowOperatorHandler)
        : windowOperatorHandler(windowOperatorHandler){};

    std::shared_ptr<PreAggregationWindowHandler<uint64_t, uint64_t>> windowOperatorHandler;

    virtual uint32_t stop(Runtime::Execution::PipelineExecutionContext&) {
        std::cout << " stop MergeSliceWindowOperator" << std::endl;
        return 0;
    }

    ExecutionResult execute(Runtime::TupleBuffer& inputTupleBuffer,
                            Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
                            Runtime::WorkerContext& workerContext) {

        auto* mergeTask = inputTupleBuffer.getBuffer<PartitionMergeTask>();
        // auto partitions = std::move(mergeTask->partitions);
        auto partitions = windowOperatorHandler->preAggregateSliceStaging.getPartition(mergeTask->sliceIndex);
        // std::cout << "Merge Task P:" << partitions->size() << std::endl;

        auto& globalPartition = windowOperatorHandler->globalSliceStore.getPartition(mergeTask->partitionIndex);
        auto& globalSlice = globalPartition->getSlice(mergeTask->sliceIndex);
        // std::cout << "Merge ThreadLocal Sclices from P:" << mergeTask->partitionIndex << " S:" << mergeTask->sliceIndex
        //         << std::endl;
        for (auto& partition : (*partitions.get())) {
            for (uint64_t index = 0; index < partition->size(); index++) {
                auto* partitionEntry = (*partition)[index];
                auto* globalEntry = globalSlice->getEntry(partitionEntry->key);
                // merge into global value
                globalEntry->value = globalEntry->value + partitionEntry->value;
            }
        }

        // remove all partitions as we have merged all
        //std::cout << "Clean Partition:" << partitions->size() << std::endl;
        //globalPartition->removeSlicesTill(maxTriggeredSliceIndex);

        //globalSlice->getPartitions().clear();

        auto newMaxSliceIndex = globalPartition->triggeredSliceSequenceLog->append(mergeTask->sliceIndex + 1);
        auto currentMaxSliceIndex = globalPartition->maxSliceIndex.load();
        while (newMaxSliceIndex > currentMaxSliceIndex) {
            const std::lock_guard<std::mutex> lock(globalPartition->mutex);
            if (globalPartition->maxSliceIndex.compare_exchange_strong(currentMaxSliceIndex, newMaxSliceIndex)) {
                // we updated the slice index from currentMaxSliceIndex to newMaxSliceIndex.
                // trigger window aggregation for all slices between currentMaxSliceIndex and newMaxSliceIndex
                // std::cout << "Send merge window " << std::endl;

                auto buffer = workerContext.allocateTupleBuffer();
                auto task = buffer.getBuffer<WindowAggregateTask>();
                task->partitionIndex = mergeTask->partitionIndex;
                task->startSliceIndex = currentMaxSliceIndex;
                task->endSliceIndex = newMaxSliceIndex;
                task->triggerSequenceNumber = globalPartition->triggeredSlices++;
                buffer.setNumberOfTuples(1);
                pipelineExecutionContext.dispatchBuffer(buffer);
            }
            currentMaxSliceIndex = globalPartition->maxSliceIndex.load();
        }

        return ExecutionResult::Ok;
    }
};

class WindowAggregateOperator : public Runtime::Execution::ExecutablePipelineStage {
  public:
    WindowAggregateOperator(std::shared_ptr<PreAggregationWindowHandler<uint64_t, uint64_t>> windowOperatorHandler)
        : windowOperatorHandler(windowOperatorHandler){};
    std::shared_ptr<PreAggregationWindowHandler<uint64_t, uint64_t>> windowOperatorHandler;

    virtual uint32_t stop(Runtime::Execution::PipelineExecutionContext&) {
        std::cout << " stop WindowAggregateOperator" << std::endl;
        return 0;
    }

    ExecutionResult execute(Runtime::TupleBuffer& inputTupleBuffer,
                            Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
                            Runtime::WorkerContext& workerContext) {

        auto resultBuffer = workerContext.allocateTupleBuffer();
        auto bufferCapacity = (resultBuffer.getBufferSize() / sizeof(WindowResultRecord));
        auto windowResult = resultBuffer.getBuffer<WindowResultRecord>();

        auto* windowAggregateTask = inputTupleBuffer.getBuffer<WindowAggregateTask>();
        // std::cout << "Got  windowAggregateTask partition: " << windowAggregateTask->partitionIndex
        //           << " endSliceIndex: " << windowAggregateTask->endSliceIndex << std::endl;
        auto& partition = windowOperatorHandler->globalSliceStore.getPartition(windowAggregateTask->partitionIndex);

        uint64_t slicesPerWindow = 1;
        // trigger window aggregation for all slices between currentMaxSliceIndex and newMaxSliceIndex
        for (uint64_t sliceIndex = windowAggregateTask->startSliceIndex; sliceIndex < windowAggregateTask->endSliceIndex;
             sliceIndex++) {
            uint64_t startSliceForWindow = sliceIndex - (slicesPerWindow - 1);
            uint64_t endSliceForWindow = sliceIndex;

            PartitionedHashMap<uint64_t, uint64_t, 1> windowState =
                PartitionedHashMap<uint64_t, uint64_t, 1>(windowOperatorHandler->bufferManager);

            // iterate over all slices that are part of this window
            for (uint64_t index = startSliceForWindow; index <= endSliceForWindow; index++) {
                if (index < partition->windowTriggerProcessor->getCurrentWatermark()) {
                    throw Compiler::CompilerException("Slice Index " + std::to_string(index) + "is already removed");
                }
                auto& sliceState = partition->getSlice(index);

                // iterate sequentially over state of slice
                for (const auto& p : sliceState->getPartitions()) {
                    for (uint64_t i = 0; i < p->size(); i++) {
                        auto* entryInSlice = (*p)[i];
                        auto* entryInWindow = windowState.getEntry(entryInSlice->key);
                        entryInWindow->value = entryInWindow->value + entryInSlice->value;
                    }
                }
            }

            auto windowStart = startSliceForWindow * windowOperatorHandler->sliceSize;
            auto windowEnd = (endSliceForWindow + 1) * windowOperatorHandler->sliceSize;
            //   std::cout << "Emit window (" << windowStart << "-" << windowEnd << ")" << std::endl;

            // output window

            for (const auto& p : windowState.getPartitions()) {
                for (uint64_t i = 0; i < p->size(); i++) {
                    auto* entryInSlice = (*p)[i];
                    windowResult[resultBuffer.getNumberOfTuples()].windowStart = windowStart;
                    windowResult[resultBuffer.getNumberOfTuples()].windowEnd = windowEnd;
                    windowResult[resultBuffer.getNumberOfTuples()].key = entryInSlice->key;
                    windowResult[resultBuffer.getNumberOfTuples()].aggregateValue = entryInSlice->value;

                    resultBuffer.setNumberOfTuples(resultBuffer.getNumberOfTuples() + 1);
                    if (resultBuffer.getNumberOfTuples() == bufferCapacity) {
                        pipelineExecutionContext.emitBuffer(resultBuffer, workerContext);
                        resultBuffer = workerContext.allocateTupleBuffer();
                    }
                }
            }
        }

        if (resultBuffer.getNumberOfTuples() != 0) {
            pipelineExecutionContext.emitBuffer(resultBuffer, workerContext);
        }

        // We triggered all windows that ended at windowAggregateTask->endSliceIndex.
        // Thus, we can drop all slices from the global slice store that are not part of any future windows.
        // This is windowAggregateTask->endSliceIndex - slicesPerWindow.
        // However, we have to make sure that for this partition the slice index is only increasing sequentially.
        auto maxTriggeredSliceIndex =
            partition->windowTriggerProcessor->updateWatermark(windowAggregateTask->endSliceIndex,
                                                               windowAggregateTask->triggerSequenceNumber);
        //std::cout << "Remove Slice from slice store:" << maxTriggeredSliceIndex << std::endl;

        partition->removeSlicesTill(maxTriggeredSliceIndex);
        return ExecutionResult::Ok;
    }
};

}// namespace NES::Experimental

#endif// NES_BENCHMARK_INCLUDE_MICROBENCHMARKS_WINDOWING_PARTITIONEDPREAGGREGATEWINDOWOPERATOR_HPP_
