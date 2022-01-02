
#include <Runtime/WorkerContext.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedEventTimeWindowHandler.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedSlice.hpp>
#include <Windowing/Experimental/TimeBasedWindow/SliceStaging.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
namespace NES::Windowing::Experimental {

KeyedEventTimeWindowHandler::KeyedEventTimeWindowHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition)
    : windowDefinition(windowDefinition) {
    watermarkProcessor =
        NES::Experimental::LockFreeMultiOriginWatermarkProcessor::create(windowDefinition->getNumberOfInputEdges());

    if (windowDefinition->getWindowType()->isTumblingWindow()) {
        auto tumblingWindow = dynamic_pointer_cast<Windowing::TumblingWindow>(windowDefinition->getWindowType());
        // auto numberOfSlices = windowDefinition->getAllowedLateness() / tumblingWindow->getSize().getTime();
        sliceSize = tumblingWindow->getSize().getTime();
    } else {
        auto slidingWindow = dynamic_pointer_cast<Windowing::SlidingWindow>(windowDefinition->getWindowType());
        // auto numberOfSlices = windowDefinition->getAllowedLateness() / tumblingWindow->getSize().getTime();
        sliceSize = slidingWindow->getSlide().getTime();
    }
}

void KeyedEventTimeWindowHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx,
                                        NES::Experimental::HashMapFactoryPtr hashmapFactory) {
    for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++) {
        threadLocalSliceStores.emplace_back(hashmapFactory, sliceSize, 100);
    }
    this->factory = hashmapFactory;
}

NES::Experimental::Hashmap KeyedEventTimeWindowHandler::getHashMap() { return factory->create(); }

void KeyedEventTimeWindowHandler::triggerThreadLocalState(Runtime::WorkerContext& wctx,
                                                          Runtime::Execution::PipelineExecutionContext& ctx,
                                                          uint64_t workerId,
                                                          uint64_t originId,
                                                          uint64_t sequenceNumber,
                                                          uint64_t watermarkTs) {

    auto& threadLocalSliceStore = getThreadLocalSliceStore(workerId);

    // the watermark update is an atomic process and returns the last and the current watermark.
    auto newGlobalWatermark = watermarkProcessor->updateWatermark(watermarkTs, sequenceNumber, originId);
    // check if the current max watermark is larger than the thread local watermark
    if (newGlobalWatermark > threadLocalSliceStore.getLastWatermark()) {

        if (threadLocalSliceStore.getLastWatermark() == 0) {
            // special case for the first watermark handling
            auto currentSliceIndex = newGlobalWatermark / sliceSize;
            threadLocalSliceStore.setFirstSliceIndex(currentSliceIndex - 1);
        }
        // push the local slices to the global slice store.
        auto firstIndex = threadLocalSliceStore.getFirstIndex();
        auto lastIndex = threadLocalSliceStore.getLastIndex();
        for (uint64_t si = firstIndex; si <= lastIndex; si++) {

            const auto& slice = threadLocalSliceStore[si];
            if (slice->getEnd() >= newGlobalWatermark) {
                break;
            }

            // put partitions to global slice store
            auto& sliceState = slice->getState();
            // each worker adds its local state to the staging area
            auto [addedPartitionsToSlice, numberOfBuffers] = sliceStaging.addToSlice(slice->getIndex(), sliceState.getEntries());
            if (addedPartitionsToSlice == threadLocalSliceStores.size()) {
                if (numberOfBuffers != 0) {
                    NES_DEBUG("Deploy merge task for slice " << slice->getIndex() << " with " << numberOfBuffers << " buffers.");
                    auto buffer = wctx.allocateTupleBuffer();
                    auto task = buffer.getBuffer<SliceMergeTask>();
                    task->sliceIndex = slice->getIndex();
                    buffer.setNumberOfTuples(1);
                    ctx.dispatchBuffer(buffer);
                } else {
                    NES_DEBUG("Slice " << slice->getIndex() << " is empty. Don't deploy merge task.");
                }
            }

            // erase slice from thread local slice store
            threadLocalSliceStore.dropFirstSlice();
        }
        threadLocalSliceStore.setLastWatermark(newGlobalWatermark);
    }
}
Windowing::LogicalWindowDefinitionPtr KeyedEventTimeWindowHandler::getWindowDefinition() { return windowDefinition; }

void KeyedEventTimeWindowHandler::start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {
    isRunning = true;
}

void KeyedEventTimeWindowHandler::stop(Runtime::Execution::PipelineExecutionContextPtr) {
    bool current = true;
    if (isRunning.compare_exchange_strong(current, false)) {
        NES_DEBUG("stop KeyedEventTimeWindowHandler");
        // todo fix shutdown, currently KeyedEventTimeWindowHandler is never destructed because operators have references.
        this->threadLocalSliceStores.clear();
    }
}
KeyedSlicePtr KeyedEventTimeWindowHandler::createKeyedSlice(uint64_t sliceIndex) {
    auto startTs = sliceIndex * sliceSize;
    auto endTs = (sliceIndex + 1) * sliceSize;
    return std::make_unique<KeyedSlice>(factory, startTs, endTs, sliceIndex);
};

}// namespace NES::Windowing::Experimental