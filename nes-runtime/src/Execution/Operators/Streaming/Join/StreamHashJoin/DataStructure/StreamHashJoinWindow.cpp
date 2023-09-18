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
#include <Util/Common.hpp>

namespace NES::Runtime::Execution {

Operators::StreamJoinHashTable* StreamHashJoinWindow::getHashTable(QueryCompilation::JoinBuildSideType joinBuildSide,
                                                                   uint64_t workerId) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING
        || joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE) {
        workerId = 0;
    }

    if (joinBuildSide == QueryCompilation::JoinBuildSideType::Left) {
        workerId = workerId % hashTableLeftSide.size();
        return hashTableLeftSide.at(workerId).get();
    } else {
        workerId = workerId % hashTableRightSide.size();
        return hashTableRightSide.at(workerId).get();
    }
}

uint64_t StreamHashJoinWindow::getNumberOfTuplesOfWorker(QueryCompilation::JoinBuildSideType joinBuildSide, uint64_t workerIdx) {
    return getHashTable(joinBuildSide, workerIdx)->getNumberOfTuples();
}

Operators::MergingHashTable& StreamHashJoinWindow::getMergingHashTable(QueryCompilation::JoinBuildSideType joinBuildSide) {
    if (joinBuildSide == QueryCompilation::JoinBuildSideType::Left) {
        return mergingHashTableLeftSide;
    } else {
        return mergingHashTableRightSide;
    }
}

StreamHashJoinWindow::StreamHashJoinWindow(size_t numberOfWorker,
                                           uint64_t windowStart,
                                           uint64_t windowEnd,
                                           size_t sizeOfRecordLeft,
                                           size_t sizeOfRecordRight,
                                           size_t maxHashTableSize,
                                           size_t pageSize,
                                           size_t preAllocPageSizeCnt,
                                           size_t numPartitions,
                                           QueryCompilation::StreamJoinStrategy joinStrategy)
    : StreamWindow(windowStart, windowEnd), mergingHashTableLeftSide(Operators::MergingHashTable(numPartitions)),
      mergingHashTableRightSide(Operators::MergingHashTable(numPartitions)), fixedPagesAllocator(maxHashTableSize),
      partitionFinishedCounter(numPartitions), joinStrategy(joinStrategy) {

    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL) {
        //TODO they all take the same allocator
        for (auto i = 0UL; i < numberOfWorker; ++i) {
            hashTableLeftSide.emplace_back(std::make_unique<Operators::LocalHashTable>(sizeOfRecordLeft,
                                                                                       numPartitions,
                                                                                       fixedPagesAllocator,
                                                                                       pageSize,
                                                                                       preAllocPageSizeCnt));
        }

        for (auto i = 0UL; i < numberOfWorker; ++i) {
            hashTableRightSide.emplace_back(std::make_unique<Operators::LocalHashTable>(sizeOfRecordRight,
                                                                                        numPartitions,
                                                                                        fixedPagesAllocator,
                                                                                        pageSize,
                                                                                        preAllocPageSizeCnt));
        }
    } else if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING) {
        hashTableLeftSide.emplace_back(std::make_unique<Operators::GlobalHashTableLocking>(sizeOfRecordLeft,
                                                                                           numPartitions,
                                                                                           fixedPagesAllocator,
                                                                                           pageSize,
                                                                                           preAllocPageSizeCnt));

        hashTableRightSide.emplace_back(std::make_unique<Operators::GlobalHashTableLocking>(sizeOfRecordRight,
                                                                                            numPartitions,
                                                                                            fixedPagesAllocator,
                                                                                            pageSize,
                                                                                            preAllocPageSizeCnt));
    } else if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE) {
        hashTableLeftSide.emplace_back(std::make_unique<Operators::GlobalHashTableLockFree>(sizeOfRecordLeft,
                                                                                            numPartitions,
                                                                                            fixedPagesAllocator,
                                                                                            pageSize,
                                                                                            preAllocPageSizeCnt));

        hashTableRightSide.emplace_back(std::make_unique<Operators::GlobalHashTableLockFree>(sizeOfRecordRight,
                                                                                             numPartitions,
                                                                                             fixedPagesAllocator,
                                                                                             pageSize,
                                                                                             preAllocPageSizeCnt));
    } else {
        NES_NOT_IMPLEMENTED();
    }

    NES_DEBUG("Create new StreamHashJoinWindow with numberOfWorkerThreads={} HTs with numPartitions={} of pageSize={} "
              "sizeOfRecordLeft={} sizeOfRecordRight={}",
              numberOfWorker,
              numPartitions,
              pageSize,
              sizeOfRecordLeft,
              sizeOfRecordRight);
}

bool StreamHashJoinWindow::markPartitionAsFinished() { return partitionFinishedCounter.fetch_sub(1) == 1; }

std::string StreamHashJoinWindow::toString() {
    std::ostringstream basicOstringstream;
    basicOstringstream << "StreamHashJoinWindow(windowState: "
                       << " windowStart: " << windowStart << " windowEnd: " << windowEnd << ")";
    return basicOstringstream.str();
}

uint64_t StreamHashJoinWindow::getNumberOfTuplesLeft() {
    uint64_t sum = 0;
    for (auto& hashTable : hashTableLeftSide) {
        sum += hashTable->getNumberOfTuples();
    }
    return sum;
}

uint64_t StreamHashJoinWindow::getNumberOfTuplesRight() {
    uint64_t sum = 0;
    for (auto& hashTable : hashTableRightSide) {
        sum += hashTable->getNumberOfTuples();
    }
    return sum;
}

}// namespace NES::Runtime::Execution