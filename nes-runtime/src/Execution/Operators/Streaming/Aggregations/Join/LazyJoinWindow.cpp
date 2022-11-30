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
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinWindow.hpp>

namespace NES::Runtime::Execution {



Operators::LocalHashTable& LazyJoinWindow::getLocalHashTable(size_t index, bool leftSide) {
    if (leftSide) {
        return localHashTableLeftSide[index];
    } else {
        return localHashTableRightSide[index];
    }
}


Operators::SharedJoinHashTable& LazyJoinWindow::getSharedJoinHashTable(bool isLeftSide) {
    if (isLeftSide) {
        return leftSideHashTable;
    } else {
        return rightSideHashTable;
    }
}

uint64_t LazyJoinWindow::fetchSubBuild(uint64_t sub) {
    return counterFinishedBuilding.fetch_sub(sub);
}

LazyJoinWindow::LazyJoinWindow(size_t maxNoWorkerThreads, uint64_t counterFinishedBuildingStart, uint64_t counterFinishedSinkStart,
                               size_t totalSizeForDataStructures, size_t sizeOfRecordLeft, size_t sizeOfRecordRight,
                               size_t lastTupleTimeStamp) : lastTupleTimeStamp(lastTupleTimeStamp) {

    counterFinishedBuilding.store(counterFinishedBuildingStart);
    counterFinishedSink.store(counterFinishedSinkStart);

    head = detail::allocHugePages<uint8_t>(totalSizeForDataStructures);
    overrunAddress = reinterpret_cast<uintptr_t>(head) + totalSizeForDataStructures;
    tail.store(reinterpret_cast<uintptr_t>(head));

    localHashTableLeftSide.reserve(maxNoWorkerThreads);
    localHashTableRightSide.reserve(maxNoWorkerThreads);

    for (auto i = 0UL; i < maxNoWorkerThreads; ++i) {
        localHashTableLeftSide.emplace_back(Operators::LocalHashTable(sizeOfRecordLeft, NUM_PARTITIONS, tail, overrunAddress));
        localHashTableRightSide.emplace_back(Operators::LocalHashTable(sizeOfRecordRight, NUM_PARTITIONS, tail, overrunAddress));
    }
}

LazyJoinWindow::~LazyJoinWindow() {
    std::free(head);
}

uint64_t LazyJoinWindow::fetchSubSink(uint64_t sub) {
    return counterFinishedSink.fetch_sub(sub);
}

size_t LazyJoinWindow::getLastTupleTimeStamp() const { return lastTupleTimeStamp; }
} // namespace NES::Runtime::Execution