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
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/StreamHashJoinWindow.hpp>

namespace NES::Runtime::Execution {

Operators::LocalHashTable* StreamHashJoinWindow::getLocalHashTable(size_t index, bool leftSide) {
    if (joinStrategy == JoinStrategy::HASH_JOIN_GLOBAL) {
        index = 0;
    }

    if (leftSide) {
        index = index % localHashTableLeftSide.size();
        return localHashTableLeftSide[index].get();
    } else {
        index = index % localHashTableRightSide.size();
        return localHashTableRightSide[index].get();
    }
}

size_t StreamHashJoinWindow::getNumberOfTuples(size_t, bool) {
    //TODO: implement this function
    size_t cnt = 0;
    for (auto& leftBucket : localHashTableLeftSide) {
        cnt += leftBucket->getNumberOfTuples();
    }
    for (auto& rightBucket : localHashTableRightSide) {
        cnt += rightBucket->getNumberOfTuples();
    }

    return cnt;
}

Operators::SharedJoinHashTable& StreamHashJoinWindow::getSharedJoinHashTable(bool isLeftSide) {
    if (isLeftSide) {
        return leftSideHashTable;
    } else {
        return rightSideHashTable;
    }
}

StreamHashJoinWindow::StreamHashJoinWindow(size_t numberOfWorker,
                                           size_t sizeOfRecordLeft,
                                           size_t sizeOfRecordRight,
                                           uint64_t windowStart,
                                           uint64_t windowEnd,
                                           size_t maxHashTableSize,
                                           size_t pageSize,
                                           size_t preAllocPageSizeCnt,
                                           size_t numPartitions,
                                           JoinStrategy joinStrategy)
    : StreamWindow(windowStart, windowEnd), numberOfWorker(numberOfWorker),
      leftSideHashTable(Operators::SharedJoinHashTable(numPartitions)),
      rightSideHashTable(Operators::SharedJoinHashTable(numPartitions)), fixedPagesAllocator(maxHashTableSize),
      partitionFinishedCounter(numPartitions), joinStrategy(joinStrategy) {

    if (joinStrategy == JoinStrategy::HASH_JOIN_LOCAL) {

        //TODO they all take the same allocator
        for (auto i = 0UL; i < numberOfWorker; ++i) {
            localHashTableLeftSide.emplace_back(std::make_unique<Operators::LocalHashTable>(sizeOfRecordLeft,
                                                                                            numPartitions,
                                                                                            fixedPagesAllocator,
                                                                                            pageSize,
                                                                                            preAllocPageSizeCnt,
                                                                                            joinStrategy));
        }

        for (auto i = 0UL; i < numberOfWorker; ++i) {
            localHashTableRightSide.emplace_back(std::make_unique<Operators::LocalHashTable>(sizeOfRecordRight,
                                                                                             numPartitions,
                                                                                             fixedPagesAllocator,
                                                                                             pageSize,
                                                                                             preAllocPageSizeCnt,
                                                                                             joinStrategy));
        }
    } else {
        localHashTableLeftSide.emplace_back(std::make_unique<Operators::LocalHashTable>(sizeOfRecordLeft,
                                                                                        numPartitions,
                                                                                        fixedPagesAllocator,
                                                                                        pageSize,
                                                                                        preAllocPageSizeCnt,
                                                                                        joinStrategy));

        localHashTableRightSide.emplace_back(std::make_unique<Operators::LocalHashTable>(sizeOfRecordRight,
                                                                                         numPartitions,
                                                                                         fixedPagesAllocator,
                                                                                         pageSize,
                                                                                         preAllocPageSizeCnt,
                                                                                         joinStrategy));
    }
    NES_DEBUG2("Create new StreamHashJoinWindow with numberOfWorkerThreads={} HTs with numPartitions={} of pageSize={} "
               "sizeOfRecordLeft={} sizeOfRecordRight={}",
               numberOfWorker,
               numPartitions,
               pageSize,
               sizeOfRecordLeft,
               sizeOfRecordRight);
}

bool StreamHashJoinWindow::markPartionAsFinished() { return partitionFinishedCounter.fetch_sub(1) == 1; }

std::string StreamHashJoinWindow::toString() {
    std::ostringstream basicOstringstream;
    basicOstringstream << "StreamHashJoinWindow(windowState: "
                       << " windowStart: " << windowStart << " windowEnd: " << windowEnd << ")";
    return basicOstringstream.str();
}

}// namespace NES::Runtime::Execution