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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_NLJPROBE_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_NLJPROBE_HPP_

#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinProbe.hpp>

namespace NES::Runtime::Execution::Operators {
class NLJProbe : public StreamJoinProbe {
  public:
    /**
     * @brief Constructor for a NLJProbe join phase
     * @param operatorHandlerIndex
     * @param joinSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param windowMetaData
     * @param leftEntrySize
     * @param rightEntrySize
     * @param joinStrategy
     * @param windowingStrategy
     * @param withDeletion
     */
    NLJProbe(const uint64_t operatorHandlerIndex,
             const JoinSchema& joinSchema,
             const std::string& joinFieldNameLeft,
             const std::string& joinFieldNameRight,
             const WindowMetaData& windowMetaData,
             const uint64_t leftEntrySize,
             const uint64_t rightEntrySize,
             QueryCompilation::StreamJoinStrategy joinStrategy,
             QueryCompilation::WindowingStrategy windowingStrategy,
             bool withDeletion = true);

    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  protected:
    const uint64_t leftEntrySize;
    const uint64_t rightEntrySize;
    const MemoryProvider::MemoryProviderPtr leftMemProvider;
    const MemoryProvider::MemoryProviderPtr rightMemProvider;
};
};    // namespace NES::Runtime::Execution::Operators
#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_NLJPROBE_HPP_
