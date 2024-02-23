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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_BUCKETING_NLJOPERATORHANDLERBUCKETING_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_BUCKETING_NLJOPERATORHANDLERBUCKETING_HPP_

#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandlerBucketing.hpp>

namespace NES::Runtime::Execution::Operators {

class NLJOperatorHandlerBucketing : public NLJOperatorHandler, public StreamJoinOperatorHandlerBucketing {
  public:
    /**
     * @brief Constructor for a NLJOperatorHandlerBucketing
     * @param inputOrigins
     * @param outputOriginId
     * @param windowSize
     * @param windowSlide
     * @param sizeOfRecordLeft
     * @param sizeOfRecordRight
     * @param pageSizeLeft
     * @param pageSizeRight
     */
    NLJOperatorHandlerBucketing(const std::vector<OriginId>& inputOrigins,
                                const OriginId outputOriginId,
                                const uint64_t windowSize,
                                const uint64_t windowSlide,
                                const SchemaPtr& leftSchema,
                                const SchemaPtr& rightSchema,
                                const uint64_t pageSizeLeft,
                                const uint64_t pageSizeRight);

    /**
     * @brief Creates a NLJOperatorHandlerBucketing
     * @param inputOrigins
     * @param outputOriginId
     * @param windowSize
     * @param windowSlide
     * @param sizeOfRecordLeft
     * @param sizeOfRecordRight
     * @param pageSizeLeft
     * @param pageSizeRight
     * @return NLJOperatorHandlerPtr
     */
    static NLJOperatorHandlerPtr create(const std::vector<OriginId>& inputOrigins,
                                        const OriginId outputOriginId,
                                        const uint64_t windowSize,
                                        const uint64_t windowSlide,
                                        const SchemaPtr& leftSchema,
                                        const SchemaPtr& rightSchema,
                                        const uint64_t pageSizeLeft,
                                        const uint64_t pageSizeRight);
};
}// namespace NES::Runtime::Execution::Operators

#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_BUCKETING_NLJOPERATORHANDLERBUCKETING_HPP_
