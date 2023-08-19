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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_JOINPHASES_STREAMHASHJOINPROBE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_JOINPHASES_STREAMHASHJOINPROBE_HPP_
#include <Execution/Operators/Operator.hpp>

namespace NES {
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;
}// namespace NES

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class is the second and final phase of the StreamJoin. For each bucket in the SharedHashTable, we iterate
 * through both buckets and check via the BloomFilter if a given key is in the bucket. If this is the case, the corresponding
 * tuples will be joined together and emitted.
 */
class StreamHashJoinProbe : public Operator {

  public:
    /**
     * @brief Constructor for a StreamJoinSink
     * @param handlerIndex
     * @param joinSchemaLeft
     * @param joinFieldNameRight
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param joinFieldNameOutput
     */

    explicit StreamHashJoinProbe(uint64_t handlerIndex,
                                 SchemaPtr joinSchemaLeft,
                                 SchemaPtr joinSchemaRight,
                                 SchemaPtr joinSchemaOutput,
                                 std::string joinFieldNameLeft,
                                 std::string joinFieldNameRight);

    /**
     * @brief receives a record buffer and then performs the join for the corresponding bucket. Currently,
     * this method emits a buffer
     * @param executionCtx
     * @param recordBuffer
     */
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    void setDeletion(bool deletion) { withDeletion = deletion; }

  protected:
    bool withDeletion = true;

  private:
    uint64_t handlerIndex;
    SchemaPtr joinSchemaLeft;
    SchemaPtr joinSchemaRight;
    SchemaPtr joinSchemaOutput;
    std::string joinFieldNameLeft;
    std::string joinFieldNameRight;
};

}//namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_JOINPHASES_STREAMHASHJOINPROBE_HPP_
