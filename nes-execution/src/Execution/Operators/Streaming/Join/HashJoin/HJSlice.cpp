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

#include <Execution/Operators/Streaming/Join/HashJoin/HJSlice.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/StreamJoinHashTable.hpp>

namespace NES::Runtime::Execution {

Operators::StreamJoinHashTable* HJSlice::getHashTable(QueryCompilation::JoinBuildSideType joinBuildSide, uint64_t workerId) {
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

uint64_t HJSlice::getNumberOfTuplesOfWorker(QueryCompilation::JoinBuildSideType joinBuildSide, uint64_t workerIdx) {
    return getHashTable(joinBuildSide, workerIdx)->getNumberOfTuples();
}

Operators::MergingHashTable& HJSlice::getMergingHashTable(QueryCompilation::JoinBuildSideType joinBuildSide) {
    if (joinBuildSide == QueryCompilation::JoinBuildSideType::Left) {
        return mergingHashTableLeftSide;
    } else {
        return mergingHashTableRightSide;
    }
}

void HJSlice::mergeLocalToGlobalHashTable() {
    std::lock_guard lock(mutexMergeLocalToGlobalHashTable);
    if (alreadyMergedLocalToGlobalHashTable) {
        return;
    }
    alreadyMergedLocalToGlobalHashTable = true;

    // Merging all buckets for all local hash tables for the left side
    for (const auto& workerHashTableLeft : hashTableLeftSide) {
        for (auto bucketPos = 0_u64; bucketPos < workerHashTableLeft->getNumBuckets(); ++bucketPos) {
            mergingHashTableLeftSide.insertBucket(bucketPos, workerHashTableLeft->getBucketLinkedList(bucketPos));
        }
    }

    // Merging all buckets for all local hash tables for the right side
    for (const auto& workerHashTableRight : hashTableRightSide) {
        for (auto bucketPos = 0_u64; bucketPos < workerHashTableRight->getNumBuckets(); ++bucketPos) {
            mergingHashTableRightSide.insertBucket(bucketPos, workerHashTableRight->getBucketLinkedList(bucketPos));
        }
    }
}

HJSlice::HJSlice(size_t numberOfWorker,
                 uint64_t sliceStart,
                 uint64_t sliceEnd,
                 size_t sizeOfRecordLeft,
                 size_t sizeOfRecordRight,
                 size_t maxHashTableSize,
                 size_t pageSize,
                 size_t preAllocPageSizeCnt,
                 size_t numPartitions,
                 QueryCompilation::StreamJoinStrategy joinStrategy)
    : StreamSlice(sliceStart, sliceEnd), mergingHashTableLeftSide(Operators::MergingHashTable(numPartitions)),
      mergingHashTableRightSide(Operators::MergingHashTable(numPartitions)), fixedPagesAllocator(maxHashTableSize),
      alreadyMergedLocalToGlobalHashTable(false), joinStrategy(joinStrategy) {

    std::cout << "HJSlice for sliceStart: " << sliceStart << " and sliceEnd: " << sliceEnd << std::endl;
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

std::string HJSlice::toString() {
    std::ostringstream basicOstringstream;
    basicOstringstream << "StreamHashJoinWindow(sliceStart: " << sliceStart << " sliceEnd: " << sliceEnd << ")";
    return basicOstringstream.str();
}

uint64_t HJSlice::getNumberOfTuplesLeft() {
    uint64_t sum = 0;
    for (auto& hashTable : hashTableLeftSide) {
        sum += hashTable->getNumberOfTuples();
    }
    return sum;
}

uint64_t HJSlice::getNumberOfTuplesRight() {
    uint64_t sum = 0;
    for (auto& hashTable : hashTableRightSide) {
        sum += hashTable->getNumberOfTuples();
    }
    return sum;
}

}// namespace NES::Runtime::Execution