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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HJPROBE_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HJPROBE_HPP_

#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinProbe.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class probes the join for a triggered window. It receives a left and right window/slice (via an id) and a hash bucket id.
 * It then performs an equality check for each left and right tuple.
 */
class HJProbe : public StreamJoinProbe {
  public:
    /**
     * @brief Constructor for a HJProbe join phase
     * @param operatorHandlerIndex
     * @param joinSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param windowMetaData
     * @param joinStrategy
     * @param windowingStrategy
     * @param withDeletion
     */
    HJProbe(const uint64_t operatorHandlerIndex,
            const JoinSchema& joinSchema,
            const std::string& joinFieldNameLeft,
            const std::string& joinFieldNameRight,
            const WindowMetaData& windowMetaData,
            QueryCompilation::StreamJoinStrategy joinStrategy,
            QueryCompilation::WindowingStrategy windowingStrategy,
            bool withDeletion = true);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

  protected:
    const MemoryProvider::MemoryProviderPtr leftMemProvider;
    const MemoryProvider::MemoryProviderPtr rightMemProvider;
};
}// namespace NES::Runtime::Execution::Operators

#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_HJPROBE_HPP_
