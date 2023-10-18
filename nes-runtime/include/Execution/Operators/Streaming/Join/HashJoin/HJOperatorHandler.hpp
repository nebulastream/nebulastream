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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_HJOPERATORHANDLER_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_HJOPERATORHANDLER_HPP_

#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators {

class HJOperatorHandler;
using HJOperatorHandlerPtr = std::shared_ptr<HJOperatorHandler>;

/**
 * @brief This class provides methods that are common for slicing and bucketing HJOperatorHandlers but not common for all join strategies.
 */
class HJOperatorHandler : virtual public StreamJoinOperatorHandler {
  public:
    /**
     * @brief Constructor for a HJOperatorHandler
     * @param inputOrigins
     * @param outputOriginId
     * @param windowSize
     * @param windowSlide
     * @param sizeOfRecordLeft
     * @param sizeOfRecordRight
     * @param joinStrategy
     * @param totalSizeForDataStructures
     * @param preAllocPageSizeCnt
     * @param pageSize
     * @param numPartitions
     */
    HJOperatorHandler(const std::vector<OriginId>& inputOrigins,
                      const OriginId outputOriginId,
                      const uint64_t windowSize,
                      const uint64_t windowSlide,
                      uint64_t sizeOfRecordLeft,
                      uint64_t sizeOfRecordRight,
                      const QueryCompilation::StreamJoinStrategy joinStrategy,
                      uint64_t totalSizeForDataStructures,
                      uint64_t preAllocPageSizeCnt,
                      uint64_t pageSize,
                      uint64_t numPartitions);

    StreamSlicePtr createNewSlice(uint64_t sliceStart, uint64_t sliceEnd) override;
    void emitSliceIdsToProbe(StreamSlice& sliceLeft,
                             StreamSlice& sliceRight,
                             const WindowInfo& windowInfo,
                             PipelineExecutionContext* pipelineCtx) override;

    /**
     * @brief get the number of pre-allocated pages per bucket
     * @return
     */
    uint64_t getPreAllocPageSizeCnt() const;

    /**
     * @brief get the page size in the HT
     * @return
     */
    uint64_t getPageSize() const;

    /**
     * @brief get the number of partitions in the HT
     * @return
     */
    uint64_t getNumPartitions() const;

    /**
     * @brief get the maximal total size of the hash table
     * @return size
     */
    uint64_t getTotalSizeForDataStructures() const;

  protected:
    QueryCompilation::StreamJoinStrategy joinStrategy;
    uint64_t totalSizeForDataStructures;
    uint64_t preAllocPageSizeCnt;
    uint64_t pageSize;
    uint64_t numPartitions;
};

/**
 * @brief Inserts the key into the hash table and returns a pointer to a free memory space
 * @param ptrLocalHashTable
 * @param key
 * @return void* / MemRef
 */
void* insertFunctionProxy(void* ptrLocalHashTable, uint64_t key);
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_HJOPERATORHANDLER_HPP_
