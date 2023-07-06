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
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/DataStructure/NLJWindow.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <sstream>
namespace NES::Runtime::Execution {

NLJWindow::NLJWindow(uint64_t windowStart, uint64_t windowEnd, uint64_t numberOfWorker, uint64_t leftEntrySize,
                     uint64_t leftPageSize, uint64_t rightEntrySize, uint64_t rightPageSize)
                    : StreamWindow(windowStart, windowEnd) {
    for (uint64_t i = 0; i < numberOfWorker; ++i) {
        auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
        leftTuples.emplace_back(std::make_unique<Nautilus::Interface::PagedVector>(std::move(allocator),
                                                                                   leftEntrySize,
                                                                                   leftPageSize));
    }

    for (uint64_t i = 0; i < numberOfWorker; ++i) {
        auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
        rightTuples.emplace_back(std::make_unique<Nautilus::Interface::PagedVector>(std::move(allocator),
                                                                                    rightEntrySize,
                                                                                    rightPageSize));
    }
    NES_DEBUG2("Created NLJWindow {}", NLJWindow::toString());
}


uint64_t NLJWindow::getNumberOfTuples(bool leftSide) {
    uint64_t sum = 0;
    if (leftSide) {
        for (auto& pagedVec : leftTuples) {
            sum += pagedVec->getNumberOfEntries();
        }
    } else {
        for (auto& pagedVec : rightTuples) {
            sum += pagedVec->getNumberOfEntries();
        }
    }
    return sum;
}

std::string NLJWindow::toString() {
    std::ostringstream basicOstringstream;
    basicOstringstream << "NLJWindow(windowStart: " << windowStart
                       << " windowEnd: " << windowEnd
                       << " windowState: " << std::string(magic_enum::enum_name<WindowState>(windowState))
                       << " leftNumberOfTuples: " << getNumberOfTuples(/*isLeftSide*/ true)
                       << " rightNumberOfTuples: " << getNumberOfTuples(/*isLeftSide*/ false)
                       << ")";
    return basicOstringstream.str();
}

void* NLJWindow::getPagedVectorRef(bool leftSide, uint64_t workerId) {
    if (leftSide) {
        return leftTuples[workerId % leftTuples.size()].get();
    } else {
        return rightTuples[workerId % rightTuples.size()].get();
    }
}

void NLJWindow::combinePagedVectors() {
    // Appending all PagedVectors for the left join side
    for (uint64_t i = 1; i < leftTuples.size(); ++i) {
        leftTuples[0]->appendAllPages(*leftTuples[i]);
        leftTuples[i].release();
    }

    // Appending all PagedVectors for the right join side
    for (uint64_t i = 1; i < rightTuples.size(); ++i) {
        rightTuples[0]->appendAllPages(*rightTuples[i]);
        rightTuples[i].release();
    }
}

}// namespace NES::Runtime::Execution