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
#ifndef NES_STREAMJOINOPERATORHANDLER_HPP
#define NES_STREAMJOINOPERATORHANDLER_HPP

#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/DataStructure/LocalHashTable.hpp>
#include <Execution/Operators/Streaming/Join/DataStructure/SharedJoinHashTable.hpp>
#include <Execution/Operators/Streaming/Join/DataStructure/StreamJoinWindow.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <cstddef>
#include <list>
#include <queue>
#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * This is a description of the StreamJoin and its data structure. The stream join consists of two phases, build and sink.
 * In the build phase, each thread builds a local hash table and once it is done, e.g., seen all tuples of a window, it
 * appends its buckets to a global hash table. Once both sides (left and right) have finished appending all buckets to
 * the global hash table, a buffer will be created with the bucket id (partition id) and the corresponding window id.
 * The join sink operates on the created buffers and performs a join for the corresponding window and bucket of the window.
 *
 * Both hash tables (LocalHashTable and SharedHashTable) consist of pages (FixedPage) that store the tuples belonging to
 * a certain bucket / partition. All pages are pre-allocated and moved from the LocalHashTable to the SharedHashTable and
 * thus, the StreamJoin only has to allocate memory once.
 * @brief This class is the operator to a StreamJoin operator. It stores all data structures necessary for the two phases: build and sink
 */
class StreamJoinOperatorHandler : public OperatorHandler, public Runtime::BufferRecycler {

  public:
    /**
     * @brief Constructor for a StreamJoinOperatorHandler
     * @param joinSchemaLeft
     * @param joinSchemaRight
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param maxNoWorkerThreads
     * @param counterFinishedBuildingStart
     * @param totalSizeForDataStructures
     * @param windowSize
     * @param pageSize
     * @param numPartitions
     */
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

    /**
     * @brief Getter for the left join schema
     * @return left join schema
     */
    SchemaPtr getJoinSchemaLeft() const;

    /**
     * @brief Getter for the right join schema
     * @return right join schema
     */
    SchemaPtr getJoinSchemaRight() const;

    /**
     * @brief Getter for the field name of the left join schmea
     * @return string of the join field name left
     */
    const std::string& getJoinFieldNameLeft() const;

    /**
     * @brief Getter for the field name of the right join schmea
     * @return string of the join field name right
     */
    const std::string& getJoinFieldNameRight() const;

    /**
     * @brief Starts the operator handler
     * @param pipelineExecutionContext
     * @param stateManager
     * @param localStateVariableId
     */
    void start(PipelineExecutionContextPtr pipelineExecutionContext,
               StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    /**
     * @brief Stops the operator handler.
     * @param pipelineExecutionContext
     */
    void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Interface method for pooled buffer recycling
     */
    void recyclePooledBuffer(NES::Runtime::detail::MemorySegment*) override;

    /**
     * @brief Interface method for unpooled buffer recycling
     */
    void recycleUnpooledBuffer(NES::Runtime::detail::MemorySegment*) override;

    /**
     * @brief creating a new window either for the left or right side of the join
     * @param isLeftSide
     */
    void createNewWindow(bool isLeftSide);

    /**
     * @brief deletes the window corresponding to this window
     * @param timeStamp
     */
    void deleteWindow(uint64_t timeStamp);

    /**
     * @brief Returns the window corresponding to the timeStamp
     * @param timeStamp
     * @return Reference to the Window
     */
    StreamJoinWindow& getWindow(uint64_t timeStamp);

    /**
     * @brief Returning the number of active windows
     * @return number of active window
     */
    size_t getNumActiveWindows();

    /**
     * @brief Returns the current window that gets filled
     * @param isLeftSide
     * @return Reference to the current window that gets filled
     */
    StreamJoinWindow& getWindowToBeFilled(bool isLeftSide);

    /**
     * @brief Increments the timeStamp of either the left or right side
     * @param increment
     * @param isLeftSide
     */
    void incLastTupleTimeStamp(uint64_t increment, bool isLeftSide);

    /**
     * @brief Returns the window size
     * @return window size
     */
    size_t getWindowSize() const;

    /**
     * @brief Returns the number of buckets/partitions
     * @return number of partitions
     */
    size_t getNumPartitions() const;

    /**
     * @brief Returns the timeStamp of the last tuple for the current window either of the left or the right side
     * @param isLeftSide
     * @return timeStamp of the last tuple
     */
    uint64_t getLastTupleTimeStamp(bool isLeftSide) const;

    /**
     * @brief Checks if a window with this timeStamp exists
     * @param timeStamp
     * @return true if exists, false otherwise
     */
    bool checkWindowExists(uint64_t timeStamp);

  private:
    SchemaPtr joinSchemaLeft;
    SchemaPtr joinSchemaRight;
    std::string joinFieldNameLeft;
    std::string joinFieldNameRight;
    std::list<StreamJoinWindow> streamJoinWindows;
    size_t maxNoWorkerThreads;
    uint64_t counterFinishedBuildingStart;
    uint64_t counterFinishedSinkStart;
    size_t totalSizeForDataStructures;
    uint64_t lastTupleTimeStampLeft, lastTupleTimeStampRight;
    size_t windowSize;
    size_t pageSize;
    size_t numPartitions;
};

}// namespace NES::Runtime::Execution::Operators
#endif//NES_STREAMJOINOPERATORHANDLER_HPP
