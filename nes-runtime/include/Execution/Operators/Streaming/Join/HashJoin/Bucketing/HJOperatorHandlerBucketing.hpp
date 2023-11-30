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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_BUCKETING_HJOPERATORHANDLERBUCKETING_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_BUCKETING_HJOPERATORHANDLERBUCKETING_HPP_

#include <Execution/Operators/Streaming/Join/HashJoin/HJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandlerBucketing.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class combines the HJOperatorHandler and StreamJoinOperatorHandlerBucketing, so that a HJBuildBucketing operator can
 * use the implemented methods from both parent classes.
 */
class HJOperatorHandlerBucketing : public HJOperatorHandler, public StreamJoinOperatorHandlerBucketing {
  public:
    /**
     * @brief Constructor for a HJOperatorHandlerBucketing
     * @param inputOrigins
     * @param outputOriginId
     * @param windowSize
     * @param windowSlide
     * @param sizeOfRecordLeft: Sum of all field sizes
     * @param sizeOfRecordRight: Sum of all field sizes
     * @param joinStrategy
     * @param totalSizeForDataStructures
     * @param preAllocPageSizeCnt
     * @param pageSize
     * @param numPartitions
     */
    HJOperatorHandlerBucketing(const std::vector<OriginId>& inputOrigins,
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

    /**
     * @brief Creates a HJOperatorHandlerBucketing
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
     * @return HJOperatorHandlerPtr
     */
    static HJOperatorHandlerPtr create(const std::vector<OriginId>& inputOrigins,
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
};
}// namespace NES::Runtime::Execution::Operators

#endif  // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_BUCKETING_HJOPERATORHANDLERBUCKETING_HPP_
