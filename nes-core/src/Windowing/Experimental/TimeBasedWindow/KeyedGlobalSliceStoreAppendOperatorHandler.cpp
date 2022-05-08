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

#include <Runtime/WorkerContext.hpp>
#include <Util/NonBlockingMonotonicSeqQueue.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedGlobalSliceStoreAppendOperatorHandler.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedSlice.hpp>
#include <Windowing/Experimental/TimeBasedWindow/SliceStaging.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
namespace NES::Windowing::Experimental {

KeyedGlobalSliceStoreAppendOperatorHandler::KeyedGlobalSliceStoreAppendOperatorHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                                                                       std::weak_ptr<KeyedGlobalSliceStore> globalSliceStore)
    : globalSliceStore(globalSliceStore), windowDefinition(windowDefinition) {
    windowSize = windowDefinition->getWindowType()->getSize().getTime();
    windowSlide = windowDefinition->getWindowType()->getSlide().getTime();
}

void KeyedGlobalSliceStoreAppendOperatorHandler::setup(Runtime::Execution::PipelineExecutionContext&,
                                          NES::Experimental::HashMapFactoryPtr hashmapFactory) {
    this->factory = hashmapFactory;
}

NES::Experimental::Hashmap KeyedGlobalSliceStoreAppendOperatorHandler::getHashMap() { return factory->create(); }

void KeyedGlobalSliceStoreAppendOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {
    NES_DEBUG("start KeyedGlobalSliceStoreAppendOperatorHandler");
}

void KeyedGlobalSliceStoreAppendOperatorHandler::triggerSliceMerging(Runtime::WorkerContext& wctx,
                                                    Runtime::Execution::PipelineExecutionContext& ctx,
                                                    uint64_t sequenceNumber,
                                                    KeyedSlicePtr slice) {
    auto global = globalSliceStore.lock();
    if (!global) {
        return;
    }
    // add pre-aggregated slice to slice store
    auto [oldMaxSliceIndex, newMaxSliceIndex] = global->addSlice(sequenceNumber, slice->getEnd(), std::move(slice));
    // check if we can trigger window computation
    if (newMaxSliceIndex > oldMaxSliceIndex) {
        auto buffer = wctx.allocateTupleBuffer();
        auto task = buffer.getBuffer<WindowTriggerTask>();
        // we trigger the completion of all windows that end between startSlice and <= endSlice.
        NES_DEBUG("Deploy window trigger task for slice  ( " << oldMaxSliceIndex << "-" << newMaxSliceIndex << ")");
        task->startSlice = oldMaxSliceIndex;
        task->endSlice = newMaxSliceIndex;
        task->sequenceNumber = newMaxSliceIndex;
        buffer.setNumberOfTuples(1);
        ctx.dispatchBuffer(buffer);
    }
}

void KeyedGlobalSliceStoreAppendOperatorHandler::stop(Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("stop KeyedGlobalSliceStoreAppendOperatorHandler");

}

KeyedSlicePtr KeyedGlobalSliceStoreAppendOperatorHandler::createKeyedSlice(uint64_t sliceIndex) {
    auto startTs = sliceIndex;
    auto endTs = (sliceIndex + 1);
    return std::make_unique<KeyedSlice>(factory, startTs, endTs);
};

}// namespace NES::Windowing::Experimental