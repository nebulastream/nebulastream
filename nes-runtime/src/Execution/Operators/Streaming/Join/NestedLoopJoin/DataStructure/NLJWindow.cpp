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

#include <Execution/Operators/Streaming/Join/NestedLoopJoin/DataStructure/NLJWindow.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <sstream>
namespace NES::Runtime::Execution {

NLJWindow::NLJWindow(uint64_t windowStart, uint64_t windowEnd, uint64_t numWorkerThreads)
                    : StreamWindow(windowStart, windowEnd), numWorkerThreads(numWorkerThreads) {}


uint64_t NLJWindow::getNumberOfTuples(bool leftSide) {
    if (leftSide) {
        uint64_t sum = 0;
        for (auto& pagedVec : leftTuples) {
            sum += pagedVec->getNumberOfEntries();
        }
        return sum;
    } else {
        uint64_t sum = 0;
        for (auto& pagedVec : rightTuples) {
            sum += pagedVec->getNumberOfEntries();
        }
        return sum;
    }
}

std::string NLJWindow::toString() {
    std::ostringstream basicOstringstream;
    basicOstringstream << "NLJWindow(windowStart: " << windowStart << " windowEnd: " << windowEnd << ")";
    return basicOstringstream.str();
}

void* NLJWindow::getPagedVectorRef(bool leftSide, uint64_t workerId) {
    if (leftSide) {
        return leftTuples[workerId % numWorkerThreads].get();
    } else {
        return rightTuples[workerId % numWorkerThreads].get();
    }
}

void NLJWindow::combinePagedVectors() {
    // Combing all PagedVectors on the left side
    auto& leftSourcePagedVector = leftTuples[0];
    for (auto i = (uint64_t )1; i < leftTuples.size(); ++i) {
        leftSourcePagedVector->combinePagedVectors(*leftTuples[i]);
        leftTuples[i].release();
    }

    // Combing all PagedVectors on the right side
    auto& rightSourcePagedVector = rightTuples[0];
    for (auto i = (uint64_t )1; i < rightTuples.size(); ++i) {
        rightSourcePagedVector->combinePagedVectors(*rightTuples[i]);
        rightTuples[i].release();
    }
}

}// namespace NES::Runtime::Execution