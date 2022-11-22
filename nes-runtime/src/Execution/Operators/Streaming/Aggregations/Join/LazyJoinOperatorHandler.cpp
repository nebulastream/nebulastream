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
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinOperatorHandler.hpp>


namespace NES::Runtime::Execution {

const Operators::LocalHashTable& LazyJoinOperatorHandler::getWorkerHashTable(size_t index) const {
    NES_ASSERT2_FMT(index < workerHashTable.size(), "LazyJoinOperatorHandler tried to access local hashtable via an index that is larger than the vector!");

    return workerHashTable[index];
}


const Operators::SharedJoinHashTable& LazyJoinOperatorHandler::getSharedJoinHashTable(bool isLeftSide) const {
    if (isLeftSide) {
        return leftSideHashTable;
    } else {
        return rightSideHashTable;
    }
}

uint64_t LazyJoinOperatorHandler::fetch_sub(uint64_t sub) const {
    return counterFinishedBuilding.fetch_sub(sub);
}

SchemaPtr LazyJoinOperatorHandler::getJoinSchema() const { return joinSchema; }

} // namespace NES::Runtime::Execution