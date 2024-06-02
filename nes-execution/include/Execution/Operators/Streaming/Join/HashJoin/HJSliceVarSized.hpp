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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HJSLICEVARSIZED_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HJSLICEVARSIZED_HPP_

#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/MergingHashTableVarSized.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/StreamJoinHashTableVarSized.hpp>
#include <Execution/Operators/Streaming/Join/StreamSlice.hpp>
#include <Util/Common.hpp>

namespace NES::Runtime::Execution {

/**
 * @brief This class is a data container for all the necessary objects in a slice/window of the StreamJoin with variable
 * sized data.
 */
class HJSliceVarSized : public StreamSlice {

  public:
    /**
      * @brief Constructor for a StreamJoinWindowVarSized
      * @param numberOfWorker
      * @param sliceStart
      * @param sliceEnd
      * @param leftSchema
      * @param rightSchema
      * @param bufferManager
      * @param pageSize
      * @param numPartitions
      */
    explicit HJSliceVarSized(size_t numberOfWorker,
                             uint64_t sliceStart,
                             uint64_t sliceEnd,
                             SchemaPtr& leftSchema,
                             SchemaPtr& rightSchema,
                             BufferManagerPtr& bufferManager,
                             size_t pageSize,
                             size_t numPartitions);

    ~HJSliceVarSized() override = default;

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
     * @brief Creates a string representation of this window
     * @return String
     */
    std::string toString() override;

    /**
     * @brief Returns the local hash table of either the left or the right join side
     * @param joinBuildSide
     * @param workerThreadId
     * @return Reference to the hash table
     */
    Operators::StreamJoinHashTableVarSized* getHashTable(QueryCompilation::JoinBuildSideType joinBuildSide,
                                                         WorkerThreadId workerThreadId) const;

    /**
     * @brief Returns the shared hash table of either the left or the right side
     * @param joinBuildSide
     * @return Reference to the shared hash table
     */
    Operators::MergingHashTableVarSized& getMergingHashTable(QueryCompilation::JoinBuildSideType joinBuildSide);

    /**
     * @brief Merges all local hash tables to the global one
     */
    void mergeLocalToGlobalHashTable();

    /**
     * @brief Returns the number of tuples in this window
     * @param joinBuildSide
     * @param workerThreadId
     * @return uint64_t
     */
    uint64_t getNumberOfTuplesOfWorker(QueryCompilation::JoinBuildSideType joinBuildSide, WorkerThreadId workerThreadId) const;

  private:
    std::vector<std::unique_ptr<Operators::StreamJoinHashTableVarSized>> hashTableLeftSide;
    std::vector<std::unique_ptr<Operators::StreamJoinHashTableVarSized>> hashTableRightSide;
    Operators::MergingHashTableVarSized mergingHashTableLeftSide;
    Operators::MergingHashTableVarSized mergingHashTableRightSide;
    bool alreadyMergedLocalToGlobalHashTable;
    std::mutex mutexMergeLocalToGlobalHashTable;
};
}// namespace NES::Runtime::Execution

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HJSLICEVARSIZED_HPP_
