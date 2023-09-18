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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_STREAMHASHJOINOPERATORHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_STREAMHASHJOINOPERATORHANDLER_HPP_

#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/LocalHashTable.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/MergingHashTable.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/StreamHashJoinWindow.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Util/Common.hpp>
#include <cstddef>
#include <list>
#include <queue>
#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * This is a description of the StreamHashJoin and its data structure. The stream join consists of two phases, build and sink.
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
class StreamHashJoinOperatorHandler;
using StreamHashJoinOperatorHandlerPtr = std::shared_ptr<StreamHashJoinOperatorHandler>;
class StreamHashJoinOperatorHandler : public StreamJoinOperatorHandler {

  public:
    /**
     * @brief Constructor for a StreamJoinOperatorHandler
     * @param inputOrigins
     * @param outputOriginId
     * @param windowSize
     * @param sizeOfRecordLeft
     * @param sizeOfRecordRight
     * @param totalSizeForDataStructures
     * @param pageSize
     * @param preAllocPageSizeCnt
     * @param numPartitions
     * @param joinStrategy
     */
    explicit StreamHashJoinOperatorHandler(const std::vector<OriginId>& inputOrigins,
                                           const OriginId outputOriginId,
                                           size_t windowSize,
                                           size_t sizeOfRecordLeft,
                                           size_t sizeOfRecordRight,
                                           size_t totalSizeForDataStructures,
                                           size_t pageSize,
                                           size_t preAllocPageSizeCnt,
                                           size_t numPartitions,
                                           QueryCompilation::StreamJoinStrategy joinStrategy);

    /**
     * @brief Creates a StreamJoinOperatorHandlerPtr object
     * @param inputOrigins
     * @param outputOriginId
     * @param windowSize
     * @param sizeOfRecordLeft
     * @param sizeOfRecordRight
     * @param totalSizeForDataStructures
     * @param pageSize
     * @param preAllocPageSizeCnt
     * @param numPartitions
     * @param joinStrategy
     * @return StreamJoinOperatorHandlerPtr
     */

    static StreamHashJoinOperatorHandlerPtr create(const std::vector<OriginId>& inputOrigins,
                                                   const OriginId outputOriginId,
                                                   size_t windowSize,
                                                   size_t sizeOfRecordLeft,
                                                   size_t sizeOfRecordRight,
                                                   size_t totalSizeForDataStructures,
                                                   size_t pageSize,
                                                   size_t preAllocPageSizeCnt,
                                                   size_t numPartitions,
                                                   QueryCompilation::StreamJoinStrategy joinStrategy);

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
     * @brief get the number of prealllcated pages per bucket
     * @return
     */
    size_t getPreAllocPageSizeCnt() const;

    /**
     * @brief get the page size in the HT
     * @return
     */
    size_t getPageSize() const;

    /**
     * @brief get the number of partitins in the HT
     * @return
     */
    size_t getNumPartitions() const;

    /**
     * @brief get the maximal total size of the hash table
     * @return size
     */
    size_t getTotalSizeForDataStructures() const;

    /**
     * @brief return number of tuples in window
     * @param windowIdentifier
     * @param workerId
     * @param joinBuildSide
     * @return
     */
    uint64_t
    getNumberOfTuplesInWindow(uint64_t windowIdentifier, uint64_t workerId, QueryCompilation::JoinBuildSideType joinBuildSide);

    /**
     * @brief method to trigger the finished windows
     * @param windowIdentifiersToBeTriggered
     * @param workerCtx
     * @param pipelineCtx
     */
    void triggerWindows(std::vector<uint64_t>& windowIdentifiersToBeTriggered,
                        WorkerContext* workerCtx,
                        PipelineExecutionContext* pipelineCtx) override;

  private:
    size_t totalSizeForDataStructures;
    size_t preAllocPageSizeCnt;
    size_t pageSize;
    size_t numPartitions;
    std::vector<OperatorId> joinOperatorsId;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_STREAMHASHJOINOPERATORHANDLER_HPP_
