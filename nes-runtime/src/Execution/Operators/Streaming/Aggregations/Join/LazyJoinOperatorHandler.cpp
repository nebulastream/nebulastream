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

Operators::LocalHashTable& LazyJoinOperatorHandler::getWorkerHashTable(size_t index, bool leftSide) {
    if (leftSide) {
        return workerThreadsHashTableLeftSide[index];
    } else {
        return workerThreadsHashTableRightSide[index];
    }
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


LazyJoinOperatorHandler::LazyJoinOperatorHandler(SchemaPtr joinSchemaLeft,
                                                 SchemaPtr joinSchemaRight,
                                                 std::string joinFieldNameLeft,
                                                 std::string joinFieldNameRight,
                                                 size_t maxNoWorkerThreadsLeftSide,
                                                 size_t maxNoWorkerThreadsRightSide,
                                                 uint64_t counterFinishedBuilding,
                                                 size_t totalSizeForDataStructures)
                                                    : joinSchemaLeft(joinSchemaLeft), joinSchemaRight(joinSchemaRight),
                                                    joinFieldNameLeft(joinFieldNameLeft), joinFieldNameRight(joinFieldNameRight){

    this->counterFinishedBuilding.store(counterFinishedBuilding);

    head = detail::allocHugePages<uint8_t>(totalSizeForDataStructures);

    workerThreadsHashTableLeftSide.reserve(maxNoWorkerThreadsLeftSide);
    workerThreadsHashTableRightSide.reserve(maxNoWorkerThreadsRightSide);

    for (auto i = 0UL; i < maxNoWorkerThreadsLeftSide; ++i) {
        workerThreadsHashTableLeftSide.emplace_back(Operators::LocalHashTable(joinSchemaLeft->getSchemaSizeInBytes(),
                                                                              NUM_PARTITIONS, tail,
                                                                              overrunAddress));
    }

    for (auto i = 0UL; i < maxNoWorkerThreadsRightSide; ++i) {
        workerThreadsHashTableRightSide.emplace_back(Operators::LocalHashTable(joinSchemaRight->getSchemaSizeInBytes(),
                                                                               NUM_PARTITIONS, tail,
                                                                               overrunAddress));
    }

}

LazyJoinOperatorHandler::~LazyJoinOperatorHandler() {
    std::free(head);
}


void LazyJoinOperatorHandler::recyclePooledBuffer(Runtime::detail::MemorySegment*) {}

void LazyJoinOperatorHandler::recycleUnpooledBuffer(Runtime::detail::MemorySegment*) {}
const std::string& LazyJoinOperatorHandler::getJoinFieldNameLeft() const { return joinFieldNameLeft; }
const std::string& LazyJoinOperatorHandler::getJoinFieldNameRight() const { return joinFieldNameRight; }
SchemaPtr LazyJoinOperatorHandler::getJoinSchemaLeft() const { return joinSchemaLeft; }
SchemaPtr LazyJoinOperatorHandler::getJoinSchemaRight() const { return joinSchemaRight; }

} // namespace NES::Runtime::Execution