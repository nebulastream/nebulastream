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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_JOINPHASES_NLJPROBE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_JOINPHASES_NLJPROBE_HPP_
#include <Execution/Operators/Operator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class is the second and final phase for a nested loop join. We join the tuples via two nested loops and
 * calling the child with the newly created joined records.
 */
class NLJProbe : public Operator {
  public:
    /**
     * @brief Constructor for a NLJProbe
     * @param operatorHandlerIndex
     * @param leftSchema
     * @param rightSchema
     * @param joinSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param windowStartFieldName
     * @param windowEndFieldName
     * @param windowKeyFieldName
     */
    explicit NLJProbe(const uint64_t operatorHandlerIndex,
                      const SchemaPtr& leftSchema,
                      const SchemaPtr& rightSchema,
                      const SchemaPtr& joinSchema,
                      const uint64_t leftEntrySize,
                      const uint64_t rightEntrySize,
                      const std::string& joinFieldNameLeft,
                      const std::string& joinFieldNameRight,
                      const std::string& windowStartFieldName,
                      const std::string& windowEndFieldName,
                      const std::string& windowKeyFieldName);

    /**
     * @brief Receives a record buffer containing a slice identifiers for a left and right slice that is ready to be joined
     * @param ctx
     * @param recordBuffer
     */
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const SchemaPtr leftSchema;
    const SchemaPtr rightSchema;
    const SchemaPtr joinSchema;
    const uint64_t leftEntrySize;
    const uint64_t rightEntrySize;
    const MemoryProvider::MemoryProviderPtr leftMemProvider;
    const MemoryProvider::MemoryProviderPtr rightMemProvider;

    const std::string joinFieldNameLeft;
    const std::string joinFieldNameRight;
    const std::string windowStartFieldName;
    const std::string windowEndFieldName;
    const std::string windowKeyFieldName;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_JOINPHASES_NLJPROBE_HPP_
