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


#include <atomic>
#include <cstddef>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinUtil.hpp>


namespace NES::Runtime::Execution {

Operators::LocalHashTable& LazyJoinOperatorHandler::getWorkerHashTable(size_t index) {
    NES_ASSERT2_FMT(index < workerThreadsHashTable.size(), "LazyJoinOperatorHandler tried to access local hashtable via an index that is larger than the vector!");

    return workerThreadsHashTable[index];
}


Operators::SharedJoinHashTable& LazyJoinOperatorHandler::getSharedJoinHashTable(bool isLeftSide) {
    if (isLeftSide) {
        return leftSideHashTable;
    } else {
        return rightSideHashTable;
    }
}

uint64_t LazyJoinOperatorHandler::fetch_sub(uint64_t sub) {
    return counterFinishedBuilding.fetch_sub(sub);
}

SchemaPtr LazyJoinOperatorHandler::getJoinSchema() const { return joinSchema; }

LazyJoinOperatorHandler::LazyJoinOperatorHandler(SchemaPtr joinSchema,
                                                 size_t maxNoWorkerThreads,
                                                 uint64_t counterFinishedBuildingStart,
                                                 size_t totalSizeForDataStructures) : joinSchema(joinSchema) {
    counterFinishedBuilding.store(counterFinishedBuildingStart);

    head = detail::allocHugePages<uint8_t>(totalSizeForDataStructures);

    workerThreadsHashTable.reserve(maxNoWorkerThreads);
    for (auto i = 0UL; i < maxNoWorkerThreads; ++i) {
        workerThreadsHashTable.emplace_back(Operators::LocalHashTable(joinSchema, NUM_PARTITIONS, tail, overrunAddress));
    }

}

LazyJoinOperatorHandler::~LazyJoinOperatorHandler() {
    std::free(head);
}


void LazyJoinOperatorHandler::recyclePooledBuffer(Runtime::detail::MemorySegment*) {}

void LazyJoinOperatorHandler::recycleUnpooledBuffer(Runtime::detail::MemorySegment*) {}
const std::string& LazyJoinOperatorHandler::getJoinFieldName() const { return joinFieldName; }

} // namespace NES::Runtime::Execution