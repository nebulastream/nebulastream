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

#ifndef NES_STREAMHASHJOINWINDOW_HPP
#define NES_STREAMHASHJOINWINDOW_HPP

#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/LocalHashTable.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/SharedJoinHashTable.hpp>
#include <Execution/Operators/Streaming/Join/StreamWindow.hpp>
#include <Runtime/Allocator/FixedPagesAllocator.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <vector>

namespace NES::Runtime::Execution {

class StreamHashJoinWindow;
using StreamHashJoinWindowPtr = std::shared_ptr<StreamHashJoinWindow>;
/**
 * @brief This class is a data container for all the necessary objects in a window of the StreamJoin
 */
class StreamHashJoinWindow : public StreamWindow {

  public:
    /**
     * @brief Constructor for a StreamJoinWindow
     * @param numberOfWorkerThreads
     * @param sizeOfRecordLeft
     * @param sizeOfRecordRight
     * @param windowStart
     * @param windowEnd
     * @param pageSize
     * @param numPartitions
     */
    explicit StreamHashJoinWindow(size_t numberOfWorkerThreads,
                                  size_t sizeOfRecordLeft,
                                  size_t sizeOfRecordRight,
                                  uint64_t windowStart,
                                  uint64_t windowEnd,
                                  size_t pageSize,
                                  size_t preAllocPageSizeCnt,
                                  size_t numPartitions);

    ~StreamHashJoinWindow() = default;

    /**
     * @brief Returns the tuple
     * @param sizeOfTupleInByte
     * @param tuplePos
     * @param leftSide
     * @return Pointer to the start of the memory for the
     */
    uint8_t* getTuple(size_t sizeOfTupleInByte, size_t tuplePos, bool leftSide);

    /**
     * @brief Returns the number of tuples in this window
     * @param sizeOfTupleInByte
     * @param leftSide
     * @return size_t
     */
    size_t getNumberOfTuples(size_t sizeOfTupleInByte, bool leftSide) override;

    /**
     * @brief Creates a string representation of this window
     * @return String
     */
    std::string toString() override;

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
     * @brief this method marks that one partition of this window was finally processed by the sink
     * @param bool indicating if all partitions are done
     */
    bool markPartionAsFinished();

  private:
    uint64_t numberOfWorker;
    std::vector<std::unique_ptr<Operators::LocalHashTable>> localHashTableLeftSide;
    std::vector<std::unique_ptr<Operators::LocalHashTable>> localHashTableRightSide;
    Operators::SharedJoinHashTable leftSideHashTable;
    Operators::SharedJoinHashTable rightSideHashTable;
    Runtime::FixedPagesAllocator fixedPagesAllocator;
    std::atomic<uint64_t> partitionFinishedCounter;
};

}// namespace NES::Runtime::Execution
#endif//NES_STREAMHASHJOINWINDOW_HPP
