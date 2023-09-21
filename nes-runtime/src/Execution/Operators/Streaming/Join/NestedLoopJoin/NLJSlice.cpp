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

#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <sstream>
namespace NES::Runtime::Execution {

NLJSlice::NLJSlice(uint64_t windowStart,
                   uint64_t windowEnd,
                   uint64_t numberOfWorker,
                   uint64_t leftEntrySize,
                   uint64_t leftPageSize,
                   uint64_t rightEntrySize,
                   uint64_t rightPageSize)
    : StreamSlice(windowStart, windowEnd) {
    for (uint64_t i = 0; i < numberOfWorker; ++i) {
        auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
        leftTuples.emplace_back(
            std::make_unique<Nautilus::Interface::PagedVector>(std::move(allocator), leftEntrySize, leftPageSize));
    }

    for (uint64_t i = 0; i < numberOfWorker; ++i) {
        auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
        rightTuples.emplace_back(
            std::make_unique<Nautilus::Interface::PagedVector>(std::move(allocator), rightEntrySize, rightPageSize));
    }
    NES_TRACE("Created NLJWindow {} for {} workerThreads, resulting in {} leftTuples.size() and {} rightTuples.size()",
              NLJSlice::toString(),
              numberOfWorker,
              leftTuples.size(),
              rightTuples.size());
}

uint64_t NLJSlice::getNumberOfTuplesLeft() {
    uint64_t sum = 0;
    for (auto& pagedVec : leftTuples) {
        sum += pagedVec->getTotalNumberOfEntries();
    }
    return sum;
}

uint64_t NLJSlice::getNumberOfTuplesRight() {
    uint64_t sum = 0;
    for (auto& pagedVec : rightTuples) {
        sum += pagedVec->getTotalNumberOfEntries();
    }
    return sum;
}

std::string NLJSlice::toString() {
    std::ostringstream basicOstringstream;
    basicOstringstream << "(sliceStart: " << sliceStart << " sliceEnd: " << sliceEnd
                       << " leftNumberOfTuples: " << getNumberOfTuplesLeft()
                       << " rightNumberOfTuples: " << getNumberOfTuplesRight() << ")";
    return basicOstringstream.str();
}

void* NLJSlice::getPagedVectorRefLeft(uint64_t workerId) {
    const auto pos = workerId % leftTuples.size();
    return leftTuples[pos].get();
}

void* NLJSlice::getPagedVectorRefRight(uint64_t workerId) {
    const auto pos = workerId % rightTuples.size();
    return rightTuples[pos].get();
}

void NLJSlice::combinePagedVectors() {
    NES_TRACE("Combining pagedVectors for window: {}", this->toString());

    // Appending all PagedVectors for the left join side and removing all items except the first one
    if (leftTuples.size() > 1) {
        for (uint64_t i = 1; i < leftTuples.size(); ++i) {
            leftTuples[0]->moveAllPages(*leftTuples[i]);
        }
        leftTuples.erase(leftTuples.begin() + 1, leftTuples.end());
    }

    // Appending all PagedVectors for the right join side and removing all items except the first one
    if (rightTuples.size() > 1) {
        for (uint64_t i = 1; i < rightTuples.size(); ++i) {
            rightTuples[0]->moveAllPages(*rightTuples[i]);
        }
        rightTuples.erase(rightTuples.begin() + 1, rightTuples.end());
    }
}
} // namespace NES::Runtime::Execution