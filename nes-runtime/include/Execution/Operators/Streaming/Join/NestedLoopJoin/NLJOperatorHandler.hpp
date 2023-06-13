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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_NLJOPERATORHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_NLJOPERATORHANDLER_HPP_

#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Execution/Operators/Streaming/SliceAssigner.hpp>
#include <list>
#include <optional>

namespace NES::Runtime::Execution::Operators {
/**
 * @brief This operator handler stores multiple windows (NLJWindow) with each window containing the left and right stream tuples.
 * This class provides the two join phases (NLJBuild and NLJSink) with methods for performing a nested loop join.
 */
class NLJOperatorHandler;
using NLJOperatorHandlerPtr = std::shared_ptr<NLJOperatorHandler>;
class NLJOperatorHandler : public StreamJoinOperatorHandler {
  public:
    /**
     * @brief Constructor for a NLJOperatorHandler
     * @param origins
     * @param windowSize
     * @param sizeOfTupleInByteLeft
     * @param sizeOfTupleInByteRight
     */
    explicit NLJOperatorHandler(const std::vector<OriginId>& origins,
                                size_t windowSize,
                                uint64_t sizeOfTupleInByteLeft,
                                uint64_t sizeOfTupleInByteRight);

    ~NLJOperatorHandler() = default;
    /**
     * @brief Starts the operator handler
     * @param pipelineExecutionContext
     * @param stateManager
     * @param localStateVariableId
     */
    void start(PipelineExecutionContextPtr pipelineExecutionContext,
               StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    /**
     * @brief Stops the operator handler
     * @param terminationType
     * @param pipelineExecutionContext
     */
    void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Creates an entry for a new tuple by first getting the responsible window for the timestamp. This method is thread safe.
     * @param timestamp
     * @param isLeftSide
     * @return Pointer
     */
    uint8_t* allocateNewEntry(uint64_t timestamp, bool isLeftSide);

    /**
     * @brief Retrieves the number of tuples for a stream (left or right) and a window
     * @param windowIdentifier
     * @param isLeftSide
     * @return Number of tuples or -1 if no window exists for the window identifier
     */
    uint64_t getNumberOfTuplesInWindow(uint64_t windowIdentifier, bool isLeftSide);

    /**
     * @brief method to trigger the finished windows
     * @param windowIdentifiersToBeTriggered
     * @param workerCtx
     * @param pipelineCtx
     */
    void triggerWindows(std::vector<uint64_t> windowIdentifiersToBeTriggered,
                        WorkerContext* workerCtx,
                        PipelineExecutionContext* pipelineCtx) override;

    /**
     * @brief method to get the first tuple
     * @param windowIdentifier
     * @param isLeftSide
     * @return
     */
    uint8_t* getFirstTuple(uint64_t windowIdentifier, bool isLeftSide);

    /**
     * Create NLJ Operator handler
     * @param origins
     * @param windowSize
     * @param sizeOfTupleInByteLeft
     * @param sizeOfTupleInByteRight
     * @return
     */
    static NLJOperatorHandlerPtr create(const std::vector<OriginId>& origins,
                                        size_t windowSize,
                                        uint64_t sizeOfTupleInByteLeft,
                                        uint64_t sizeOfTupleInByteRight);

};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_NLJOPERATORHANDLER_HPP_
