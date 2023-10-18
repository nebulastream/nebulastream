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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_SLICING_HJOPERATORHANDLERSLICING_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_SLICING_HJOPERATORHANDLERSLICING_HPP_

#include <Execution/Operators/Streaming/Join/HashJoin/HJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandlerSlicing.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class combines the HJOperatorHandler and StreamJoinOperatorHandlerSlicing, so that a HJBuildBucketing operator can
 * use the implemented methods from both parent classes.
 */
class HJOperatorHandlerSlicing : public HJOperatorHandler, public StreamJoinOperatorHandlerSlicing {
  public:
    /**
     * @brief Constructor for a HJOperatorHandlerSlicing
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
    HJOperatorHandlerSlicing(const std::vector<OriginId>& inputOrigins,
                             const OriginId outputOriginId,
                             const uint64_t windowSize,
                             const uint64_t windowSlide,
                             const uint64_t sizeOfRecordLeft,
                             const uint64_t sizeOfRecordRight,
                             const QueryCompilation::StreamJoinStrategy joinStrategy,
                             const uint64_t totalSizeForDataStructures,
                             const uint64_t preAllocPageSizeCnt,
                             const uint64_t pageSize,
                             const uint64_t numPartitions);

    /**
     * @brief Creates a HJOperatorHandler
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
                                    const uint64_t sizeOfRecordLeft,
                                    const uint64_t sizeOfRecordRight,
                                    const QueryCompilation::StreamJoinStrategy joinStrategy,
                                    const uint64_t totalSizeForDataStructures,
                                    const uint64_t preAllocPageSizeCnt,
                                    const uint64_t pageSize,
                                    const uint64_t numPartitions);
};
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_SLICING_HJOPERATORHANDLERSLICING_HPP_
