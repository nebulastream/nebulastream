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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINPROBE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINPROBE_HPP_

#include <API/Schema.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/Operator.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperator.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Util/Common.hpp>
#include <Util/StdInt.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class is the second phase of the stream join. The actual implementation (nested-loops, probing hash tables)
 * is not part of this class. This class takes care of the close() functionality as this universal.
 * Furthermore, it provides a method of creating the joined tuple
 */
class StreamJoinProbe : public StreamJoinOperator, public Operator {
  public:
    /**
     * @brief Constructor for a StreamJoinProbe
     * @param operatorHandlerIndex
     * @param joinSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param windowMetaData
     * @param joinStrategy
     * @param windowingStrategy
     * @param withDeletion
     */
    StreamJoinProbe(const uint64_t operatorHandlerIndex,
                    const JoinSchema& joinSchema,
                    std::string joinFieldNameLeft,
                    std::string joinFieldNameRight,
                    const WindowMetaData& windowMetaData,
                    QueryCompilation::StreamJoinStrategy joinStrategy,
                    QueryCompilation::WindowingStrategy windowingStrategy,
                    bool withDeletion = true);

    /**
     * @brief Checks the current watermark and then deletes all slices/windows that are not valid anymore
     * @param executionCtx
     * @param recordBuffer
     */
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    /**
     * @brief Creates a joined record out of the left and right record
     * @param joinedRecord
     * @param leftRecord
     * @param rightRecord
     * @param windowStart
     * @param windowEnd
     */
    void createJoinedRecord(Record& joinedRecord,
                            Record& leftRecord,
                            Record& rightRecord,
                            const Value<UInt64>& windowStart,
                            const Value<UInt64>& windowEnd) const;

  protected:
    const uint64_t operatorHandlerIndex;
    const JoinSchema joinSchema;
    bool withDeletion;

    // TODO these will be replaced by an interface function with #3691
    const std::string joinFieldNameLeft;
    const std::string joinFieldNameRight;

    const WindowMetaData windowMetaData;
};
}// namespace NES::Runtime::Execution::Operators

#endif  // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINPROBE_HPP_
