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
#include <Execution/Operators/Relational/Join/BatchJoinHandler.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators {

BatchJoinHandler::BatchJoinHandler() = default;

Nautilus::Interface::Stack* BatchJoinHandler::getThreadLocalState(uint64_t workerId) {
    auto index = workerId % threadLocalStateStores.size();
    return threadLocalStateStores[index].get();
}

void BatchJoinHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx,
                             uint64_t entrySize,
                             uint64_t keySize,
                             uint64_t valueSize) {
    this->keySize = keySize;
    this->valueSize = valueSize;
    for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++) {
        auto allocator = std::make_unique<NesDefaultMemoryAllocator>();
        auto stack = std::make_unique<Nautilus::Interface::Stack>(std::move(allocator), entrySize);
        threadLocalStateStores.emplace_back(std::move(stack));
    }
}

Nautilus::Interface::ChainedHashMap* BatchJoinHandler::mergeState() {
    size_t numberOfKeys = 0;
    for (auto& stack : threadLocalStateStores) {
        numberOfKeys += stack->getNumberOfEntries();
    }
    auto allocator = std::make_unique<NesDefaultMemoryAllocator>();
    globalMap =
        std::make_unique<Nautilus::Interface::ChainedHashMap>(keySize, valueSize, numberOfKeys, std::move(allocator), 4096);
    for (auto& stack : threadLocalStateStores) {
        auto& pages = stack->getPages();
        NES_ASSERT(!pages.empty(), "stack should not be empty");
        for (size_t i = 0; i < pages.size() - 1; i++) {
            auto numberOfEntries = stack->capacityPerPage();
            globalMap->insertPage(pages[i], numberOfEntries);
        }
        // insert last page
        auto numberOfEntries = stack->getNumberOfEntriesOnCurrentPage();
        globalMap->insertPage(pages[pages.size() - 1], numberOfEntries);
        stack->clear();
    }
    return globalMap.get();
}

Nautilus::Interface::ChainedHashMap* BatchJoinHandler::getGlobalHashMap() { return globalMap.get(); }

void BatchJoinHandler::start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {
    NES_DEBUG("start BatchJoinHandler");
}

void BatchJoinHandler::stop(Runtime::QueryTerminationType queryTerminationType, Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("shutdown BatchJoinHandler: " << queryTerminationType);
}
BatchJoinHandler::~BatchJoinHandler() { NES_DEBUG("~BatchJoinHandler"); }

void BatchJoinHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&) {
    // this->threadLocalSliceStores.clear();
}

}// namespace NES::Runtime::Execution::Operators