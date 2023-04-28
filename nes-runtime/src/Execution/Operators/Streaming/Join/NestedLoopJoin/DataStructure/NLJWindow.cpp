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
namespace NES::Runtime::Execution {

    uint8_t* NLJWindow::insertNewTuple(size_t sizeOfTupleInByte, bool leftSide) {
        if (leftSide) {
            std::lock_guard<std::mutex> lock(leftTuplesMutex);
            auto currentSize = leftTuples.size();
            leftTuples.resize(currentSize + sizeOfTupleInByte);
            return &leftTuples[currentSize];
        } else {
            std::lock_guard<std::mutex> lock(rightTuplesMutex);
            auto currentSize = rightTuples.size();
            rightTuples.resize(currentSize + sizeOfTupleInByte);
            return &rightTuples[currentSize];
        }
    }

    uint8_t* NLJWindow::getTuple(size_t sizeOfTupleInByte, size_t tuplePos, bool leftSide) {
        if (leftSide) {
            return &leftTuples[sizeOfTupleInByte * tuplePos];
        } else {
            return &rightTuples[sizeOfTupleInByte * tuplePos];
        }
    }

    size_t NLJWindow::getNumberOfTuples(size_t sizeOfTupleInByte, bool leftSide) {
        if (leftSide) {
            return leftTuples.size() / sizeOfTupleInByte;
        } else {
            return rightTuples.size() / sizeOfTupleInByte;
        }
    }

    uint64_t NLJWindow::getWindowStart() const {
        return windowStart;
    }

    uint64_t NLJWindow::getWindowEnd() const {
        return windowEnd;
    }

    uint64_t NLJWindow::fetch_add(uint64_t add) {
        return counterFinishedBuilding.fetch_add(add);
    }
}