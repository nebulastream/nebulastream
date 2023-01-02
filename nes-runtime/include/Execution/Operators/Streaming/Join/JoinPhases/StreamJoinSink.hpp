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

#ifndef NES_STREAMJOINSINK_HPP
#define NES_STREAMJOINSINK_HPP
#include <Execution/Operators/Operator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class is the second and final phase of the StreamJoin. For each bucket in the SharedHashTable, we iterate
 * through both buckets and check via the BloomFilter if a given key is in the bucket. If this is the case, the corresponding
 * tuples will be joined together and emitted.
 */
class StreamJoinSink : public Operator {

  public:
    /**
     * @brief Constructor for a StreamJoinSink
     * @param handlerIndex
     */
    StreamJoinSink(uint64_t handlerIndex);

    /**
     * @brief receives a record buffer and then performs the join for the corresponding bucket. Currently, this method emits a buffer
     * @param executionCtx
     * @param recordBuffer
     */
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

  private:
    uint64_t handlerIndex;
};

}//namespace NES::Runtime::Execution::Operators
#endif//NES_STREAMJOINSINK_HPP
