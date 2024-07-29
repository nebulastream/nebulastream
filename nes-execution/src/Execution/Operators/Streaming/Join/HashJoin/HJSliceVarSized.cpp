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

#include <Execution/Operators/Streaming/Join/HashJoin/HJSliceVarSized.hpp>
#include <Util/StdInt.hpp>

namespace NES::Runtime::Execution {

Operators::StreamJoinHashTableVarSized* HJSliceVarSized::getHashTable(QueryCompilation::JoinBuildSideType joinBuildSide,
                                                                      WorkerThreadId workerThreadId) const {
    if (joinBuildSide == QueryCompilation::JoinBuildSideType::Left) {
        return hashTableLeftSide.at(workerThreadId % hashTableLeftSide.size()).get();
    } else {
        return hashTableRightSide.at(workerThreadId % hashTableRightSide.size()).get();
    }
}

uint64_t HJSliceVarSized::getNumberOfTuplesOfWorker(QueryCompilation::JoinBuildSideType joinBuildSide,
                                                    WorkerThreadId workerThreadId) const {
    return getHashTable(joinBuildSide, workerThreadId)->getNumberOfTuples();
}

Operators::MergingHashTableVarSized& HJSliceVarSized::getMergingHashTable(QueryCompilation::JoinBuildSideType joinBuildSide) {
    if (joinBuildSide == QueryCompilation::JoinBuildSideType::Left) {
        return mergingHashTableLeftSide;
    } else {
        return mergingHashTableRightSide;
    }
}

void HJSliceVarSized::mergeLocalToGlobalHashTable() {
    std::lock_guard lock(mutexMergeLocalToGlobalHashTable);
    if (alreadyMergedLocalToGlobalHashTable) {
        return;
    }
    alreadyMergedLocalToGlobalHashTable = true;

    for (const auto& workerHashTableLeft : hashTableLeftSide) {
        for (auto bucketPos = 0_u64; bucketPos < workerHashTableLeft->getNumBuckets(); ++bucketPos) {
            mergingHashTableLeftSide.insertBucket(bucketPos, workerHashTableLeft->getBucketPagedVector(bucketPos));
        }
    }

    for (const auto& workerHashTableRight : hashTableRightSide) {
        for (auto bucketPos = 0_u64; bucketPos < workerHashTableRight->getNumBuckets(); ++bucketPos) {
            mergingHashTableRightSide.insertBucket(bucketPos, workerHashTableRight->getBucketPagedVector(bucketPos));
        }
    }
}

HJSliceVarSized::HJSliceVarSized(size_t numberOfWorker,
                                 uint64_t sliceStart,
                                 uint64_t sliceEnd,
                                 SchemaPtr& leftSchema,
                                 SchemaPtr& rightSchema,
                                 BufferManagerPtr& bufferManager,
                                 size_t pageSize,
                                 size_t numPartitions)
    : StreamSlice(sliceStart, sliceEnd), mergingHashTableLeftSide(Operators::MergingHashTableVarSized(numPartitions)),
      mergingHashTableRightSide(Operators::MergingHashTableVarSized(numPartitions)), alreadyMergedLocalToGlobalHashTable(false) {

    for (auto i = 0UL; i < numberOfWorker; ++i) {
        hashTableLeftSide.emplace_back(
            std::make_unique<Operators::StreamJoinHashTableVarSized>(numPartitions, bufferManager, pageSize, leftSchema));
    }
    for (auto i = 0UL; i < numberOfWorker; ++i) {
        hashTableRightSide.emplace_back(
            std::make_unique<Operators::StreamJoinHashTableVarSized>(numPartitions, bufferManager, pageSize, rightSchema));
    }

    NES_DEBUG("Create new StreamHashJoinWindow with numberOfWorkerThreads={} HTs with numPartitions={} of pageSize={} ",
              numberOfWorker,
              numPartitions,
              pageSize);
}

std::string HJSliceVarSized::toString() {
    std::ostringstream basicOstringstream;
    basicOstringstream << "StreamHashJoinWindow(sliceStart: " << sliceStart << " sliceEnd: " << sliceEnd << ")";
    return basicOstringstream.str();
}

uint64_t HJSliceVarSized::getNumberOfTuplesLeft() {
    uint64_t sum = 0;
    for (auto& hashTable : hashTableLeftSide) {
        sum += hashTable->getNumberOfTuples();
    }
    return sum;
}

uint64_t HJSliceVarSized::getNumberOfTuplesRight() {
    uint64_t sum = 0;
    for (auto& hashTable : hashTableRightSide) {
        sum += hashTable->getNumberOfTuples();
    }
    return sum;
}

}// namespace NES::Runtime::Execution
