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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_REORDERTUPLEBUFFERSOPERATORHANDLER_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_REORDERTUPLEBUFFERSOPERATORHANDLER_HPP_

#include <Runtime/Execution/OperatorHandler.hpp>
#include <Sequencing/NonBlockingMonotonicSeqQueue.hpp>
#include <folly/Synchronized.h>
#include <map>

namespace NES::Runtime::Execution::Operators {

class ReorderTupleBuffersOperatorHandler;
using ReorderTupleBuffersOperatorHandlerPtr = std::shared_ptr<ReorderTupleBuffersOperatorHandler>;

class ReorderTupleBuffersOperatorHandler : public virtual OperatorHandler {
  public:
    ReorderTupleBuffersOperatorHandler();

    ~ReorderTupleBuffersOperatorHandler() override = default;

    /**
     * @brief Process next buffer and emit if its sequence number is next
     * @param ctx - pipeline execution context
     * @param wc - worker context
     * @param inputBuffer - incoming buffer
     */
    void processBuffer(PipelineExecutionContext* ctx, WorkerContext& wc, TupleBuffer& inputBuffer);

    void start(PipelineExecutionContextPtr, uint32_t) override{};
    void stop(QueryTerminationType, PipelineExecutionContextPtr) override{};

  protected:
    // Nonblocking queue to track sequence numbers of tuple buffers as they arrive out of order
    Sequencing::NonBlockingMonotonicSeqQueue<uint64_t> seqQueue;
    // keep unordered tuple buffers with sequence number as key
    folly::Synchronized<std::map<uint64_t, TupleBuffer>> bufferStorage;

    // sequence number of last written tuple buffer
    std::atomic<uint64_t> lastWritten;
    std::mutex lastWrittenMtx;
};
}// namespace NES::Runtime::Execution::Operators
#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_REORDERTUPLEBUFFERSOPERATORHANDLER_HPP_
