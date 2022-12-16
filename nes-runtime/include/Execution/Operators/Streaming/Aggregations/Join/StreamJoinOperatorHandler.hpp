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

#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/StreamJoinWindow.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LocalHashTable.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/SharedJoinHashTable.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <cstddef>
#include <queue>
#include <vector>

namespace NES::Runtime::Execution {


class StreamJoinOperatorHandler : public OperatorHandler, public Runtime::BufferRecycler {




  public:
    explicit StreamJoinOperatorHandler(SchemaPtr joinSchemaLeft,
                                     SchemaPtr joinSchemaRight,
                                     std::string joinFieldNameLeft,
                                     std::string joinFieldNameRight,
                                     size_t maxNoWorkerThreads,
                                     uint64_t counterFinishedBuildingStart,
                                     size_t totalSizeForDataStructures,
                                     size_t windowSize,
                                     size_t pageSize = CHUNK_SIZE,
                                     size_t numPartitions = NUM_PARTITIONS);

    SchemaPtr getJoinSchemaLeft() const;
    SchemaPtr getJoinSchemaRight() const;

    const std::string& getJoinFieldNameLeft() const;
    const std::string& getJoinFieldNameRight() const;

    void start(PipelineExecutionContextPtr pipelineExecutionContext,
               StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;

    void recyclePooledBuffer(NES::Runtime::detail::MemorySegment* buffer) override;
    void recycleUnpooledBuffer(NES::Runtime::detail::MemorySegment* buffer) override;

    void createNewWindow(bool isLeftSide);

    void deleteWindow(size_t timeStamp);

    StreamJoinWindow& getWindow(size_t timeStamp);

    size_t getNumActiveWindows();

    StreamJoinWindow& getWindowToBeFilled(bool isLeftSide);

    void incLastTupleTimeStamp(uint64_t increment, bool isLeftSide);

    size_t getWindowSize() const;

    size_t getNumPartitions() const;
    uint64_t getLastTupleTimeStamp(bool isLeftSide) const;

    bool checkWindowExists(size_t timeStamp);

  private:
    SchemaPtr joinSchemaLeft;
    SchemaPtr joinSchemaRight;
    std::string joinFieldNameLeft;
    std::string joinFieldNameRight;
    std::list<StreamJoinWindow> lazyJoinWindows;
    size_t maxNoWorkerThreads;
    uint64_t counterFinishedBuildingStart;
    uint64_t counterFinishedSinkStart;
    size_t totalSizeForDataStructures;
    uint64_t lastTupleTimeStampLeft, lastTupleTimeStampRight;
    size_t windowSize;
    size_t pageSize;
    size_t numPartitions;
};

} // namespace NES::Runtime::Execution::Operators
#endif//NES_LAZYJOINOPERATORHANDLER_HPP
