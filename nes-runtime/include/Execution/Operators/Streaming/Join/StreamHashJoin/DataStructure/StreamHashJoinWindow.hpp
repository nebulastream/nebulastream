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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_DATASTRUCTURE_STREAMHASHJOINWINDOW_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_DATASTRUCTURE_STREAMHASHJOINWINDOW_HPP_

#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/GlobalHashTableLockFree.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/GlobalHashTableLocking.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/LocalHashTable.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/MergingHashTable.hpp>
#include <Execution/Operators/Streaming/Join/StreamSlice.hpp>
#include <Runtime/Allocator/FixedPagesAllocator.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Common.hpp>
#include <vector>

namespace NES::Runtime::Execution {

class StreamHashJoinWindow;
using StreamHashJoinWindowPtr = std::shared_ptr<StreamHashJoinWindow>;
/**
 * @brief This class is a data container for all the necessary objects in a window of the StreamJoin
 * TODO rename this to StreamHashJoinSlice
 */
class StreamHashJoinWindow : public StreamSlice {

  public:
    /**
     * @brief Constructor for a StreamJoinWindow
     * @param numberOfWorkerThreads
     * @param sizeOfRecordLeft
     * @param sizeOfRecordRight
     * @param windowStart
     * @param windowEnd
     * @param maxHashTableSize
     * @param pageSize
     * @param preAllocPageSizeCnt
     * @param numPartitions
     */
    explicit StreamHashJoinWindow(size_t numberOfWorkerThreads,
                                  uint64_t windowStart,
                                  uint64_t windowEnd,
                                  size_t sizeOfRecordLeft,
                                  size_t sizeOfRecordRight,
                                  size_t maxHashTableSize,
                                  size_t pageSize,
                                  size_t preAllocPageSizeCnt,
                                  size_t numPartitions,
                                  QueryCompilation::StreamJoinStrategy joinStrategy);

    ~StreamHashJoinWindow() = default;

    /**
     * @brief Returns the number of tuples in this window for the left side
     * @return uint64_t
     */
    uint64_t getNumberOfTuplesLeft() override;

    /**
     * @brief Returns the number of tuples in this window for the right side
     * @return uint64_t
     */
    uint64_t getNumberOfTuplesRight() override;

    /**
     * @brief Returns the number of tuples in this window
     * @param joinBuildSide
     * @param workerIdx
     * @return uint64_t
     */
    uint64_t getNumberOfTuplesOfWorker(QueryCompilation::JoinBuildSideType joinBuildSide, uint64_t workerIdx);

    /**
     * @brief Creates a string representation of this window
     * @return String
     */
    std::string toString() override;

    /**
     * @brief Returns the local hash table of either the left or the right join side
     * @param joinBuildSide
     * @param index
     * @return Reference to the hash table
     */
    Operators::StreamJoinHashTable* getHashTable(QueryCompilation::JoinBuildSideType joinBuildSide, uint64_t index);

    /**
     * @brief Returns the shared hash table of either the left or the right side
     * @param joinBuildSide
     * @return Reference to the shared hash table
     */
    Operators::MergingHashTable& getMergingHashTable(QueryCompilation::JoinBuildSideType joinBuildSide);

    /**
     * @brief this method marks that one partition of this window was finally processed by the sink
     * @param bool indicating if all partitions are done
     */
    bool markPartitionAsFinished();

  protected:
    std::vector<std::unique_ptr<Operators::StreamJoinHashTable>> hashTableLeftSide;
    std::vector<std::unique_ptr<Operators::StreamJoinHashTable>> hashTableRightSide;
    Operators::MergingHashTable mergingHashTableLeftSide;
    Operators::MergingHashTable mergingHashTableRightSide;
    Runtime::FixedPagesAllocator fixedPagesAllocator;
    std::atomic<uint64_t> partitionFinishedCounter;
    QueryCompilation::StreamJoinStrategy joinStrategy;
};

}// namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_DATASTRUCTURE_STREAMHASHJOINWINDOW_HPP_
