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
#include <Execution/Operators/Relational/SortHandler.hpp>
#include <Nautilus/Interface/Stack/Stack.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators {

SortHandler::SortHandler() = default;

Nautilus::Interface::Stack* SortHandler::getThreadLocalState(uint64_t workerId) {
    auto index = workerId % threadLocalStateStores.size();
    return threadLocalStateStores[index].get();
}

void SortHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize) {
    for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++) {
        auto allocator = std::make_unique<NesDefaultMemoryAllocator>();
        auto stack = std::make_unique<Nautilus::Interface::Stack>(std::move(allocator), entrySize);
        threadLocalStateStores.emplace_back(std::move(stack));
    }
}

/*
Nautilus::Interface::ChainedHashMap* SortHandler::mergeState() {
    // build a global hash-map on top of all thread local stacks.
    // 1. calculate the number of total keys to size the hash-map correctly. This assumes a foreign-key join where keys can only exist one time and avoids hash coalitions
    size_t numberOfKeys = 0;
    for (auto& stack : threadLocalStateStores) {
        numberOfKeys += stack->getNumberOfEntries();
    }
    // 2. allocate hash map
    auto allocator = std::make_unique<NesDefaultMemoryAllocator>();
    globalMap = std::make_unique<Nautilus::Interface::ChainedHashMap>(keySize, valueSize, numberOfKeys, std::move(allocator), 4096);

    // 3. iterate over stacks and insert the whole page to the hash table
    // Note. This dose not perform any memory and only results in a seqential scan over all entries  plus a random lookup in the hash table.
    // TODO this look could be parallelized, but please check if needed.
    for (auto& stack : threadLocalStateStores) {
        auto& pages = stack->getPages();
        NES_ASSERT(!pages.empty(), "stack should not be empty");
        // currently we assume that page 0 - (n-1) are full and contain capacity entries.
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
}*/

void SortHandler::start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {
    NES_DEBUG("start SortHandler");
}

void SortHandler::stop(Runtime::QueryTerminationType queryTerminationType, Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("shutdown SortHandler: " << queryTerminationType);
}

SortHandler::~SortHandler() { NES_DEBUG("~SortHandler"); }

void SortHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&) {}

}// namespace NES::Runtime::Execution::Operators