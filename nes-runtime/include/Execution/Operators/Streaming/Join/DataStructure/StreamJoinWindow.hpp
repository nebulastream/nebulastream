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

#ifndef NES_STREAMJOINWINDOW_HPP
#define NES_STREAMJOINWINDOW_HPP

#include <Execution/Operators/Streaming/Join/DataStructure/LocalHashTable.hpp>
#include <Execution/Operators/Streaming/Join/DataStructure/SharedJoinHashTable.hpp>
#include <Runtime/Allocator/FixedPagesAllocator.hpp>
#include <vector>

namespace NES::Runtime::Execution {

/**
 * @brief This class is a data container for all the necessary objects in a window of the StreamJoin
 */
class StreamJoinWindow {

  public:
    /**
     * @brief Constructor for a StreamJoinWindow
     * @param maxNoWorkerThreads
     * @param counterFinishedBuildingStart
     * @param counterFinishedSinkStart
     * @param totalSizeForDataStructures
     * @param sizeOfRecordLeft
     * @param sizeOfRecordRight
     * @param lastTupleTimeStamp
     * @param pageSize
     * @param numPartitions
     */
    explicit StreamJoinWindow(size_t maxNoWorkerThreads,
                              uint64_t counterFinishedBuildingStart,
                              uint64_t counterFinishedSinkStart,
                              size_t totalSizeForDataStructures,
                              size_t sizeOfRecordLeft,
                              size_t sizeOfRecordRight,
                              uint64_t lastTupleTimeStamp,
                              size_t pageSize,
                              size_t numPartitions);

    /**
     * @brief Returns the local hash table of either the left or the right join side
     * @param index
     * @param leftSide
     * @return Reference to the hash table
     */
    Operators::LocalHashTable* getLocalHashTable(size_t index, bool leftSide);

    /**
     * @brief Returns the shared hash table of either the left or the right side
     * @param isLeftSide
     * @return Reference to the shared hash table
     */
    Operators::SharedJoinHashTable& getSharedJoinHashTable(bool isLeftSide);

    /**
     * @brief Performs an atomic fetch and sub instruction for detecting if the build phase is done
     * @param sub
     * @return old value
     */
    uint64_t fetchSubBuild(uint64_t sub);

    /**
     * @brief Performs an atomic fetch and sub instruction for detecting if the sink phase is done
     * @param sub
     * @return old value
     */
    uint64_t fetchSubSink(uint64_t sub);

    /**
     * @brief Returns the last tuple that can be inserted into this window
     * @return lastTupleTimeStamp
     */
    uint64_t getLastTupleTimeStamp() const;

  private:
    std::vector<std::unique_ptr<Operators::LocalHashTable>> localHashTableLeftSide;
    std::vector<std::unique_ptr<Operators::LocalHashTable>> localHashTableRightSide;
    Operators::SharedJoinHashTable leftSideHashTable;
    Operators::SharedJoinHashTable rightSideHashTable;
    std::atomic<uint64_t> counterFinishedBuilding;
    std::atomic<uint64_t> counterFinishedSink;
    uint64_t lastTupleTimeStamp;
    Runtime::FixedPagesAllocator fixedPagesAllocator;
};

}// namespace NES::Runtime::Execution
#endif//NES_STREAMJOINWINDOW_HPP
