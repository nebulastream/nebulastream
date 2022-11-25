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
#ifndef NES_LAZYJOINOPERATORHANDLER_HPP
#define NES_LAZYJOINOPERATORHANDLER_HPP

#include <vector>
#include <cstddef>
#include <API/Schema.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LocalHashTable.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/SharedJoinHashTable.hpp>

namespace NES::Runtime::Execution {


class LazyJoinOperatorHandler : public OperatorHandler, public Runtime::BufferRecycler {


  public:
    explicit LazyJoinOperatorHandler(SchemaPtr joinSchema,
                                     size_t maxNoWorkerThreads,
                                     uint64_t counterFinishedBuildingStart,
                                     size_t totalSizeForDataStructures);

    ~LazyJoinOperatorHandler() override;

    Operators::LocalHashTable& getWorkerHashTable(size_t index);

    Operators::SharedJoinHashTable& getSharedJoinHashTable(bool isLeftSide);

    uint64_t fetch_sub(uint64_t sub);

    SchemaPtr getJoinSchema() const;

    const std::string& getJoinFieldName() const;

    void recyclePooledBuffer(NES::Runtime::detail::MemorySegment* buffer) override;
    void recycleUnpooledBuffer(NES::Runtime::detail::MemorySegment* buffer) override;

  private:

    SchemaPtr joinSchema;
    std::string joinFieldName;
    std::vector<Operators::LocalHashTable> workerThreadsHashTable;
    Operators::SharedJoinHashTable leftSideHashTable;
    Operators::SharedJoinHashTable rightSideHashTable;
    std::atomic<uint64_t> counterFinishedBuilding;

    uint8_t* head;
    std::atomic<uint64_t> tail;
    uint64_t overrunAddress;
};

} // namespace NES::Runtime::Execution::Operators
#endif//NES_LAZYJOINOPERATORHANDLER_HPP
