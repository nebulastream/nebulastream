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
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Execution/Operators/OperatorState.hpp>
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
     */
    explicit NLJOperatorHandler(const std::vector<OriginId>& origins,
                                uint64_t sizeOfTupleInByteLeft,
                                uint64_t sizeOfTupleInByteRight,
                                uint64_t leftPageSize,
                                uint64_t rightPageSize,
                                size_t windowSize);

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
     * @brief Retrieves the pointer to paged vector for the left or right side
     * @param timestamp
     * @param leftSide
     * @return Void pointer to the pagedVector
     */
    void* getPagedVectorRef(uint64_t timestamp, bool leftSide);

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

    static NLJOperatorHandlerPtr create(const std::vector<OriginId>& origins,
                                        const uint64_t sizeOfTupleInByteLeft,
                                        const uint64_t sizeOfTupleInByteRight,
                                        const uint64_t leftPageSize,
                                        const uint64_t rightPageSize,
                                        const size_t windowSize);

    /**
     * @brief Returns the current window, by current we mean the last added window to the list. If no window exists,
     * a window with the timestamp 0 will be created
     * @return StreamWindow*
     */
    StreamWindow* getCurrentWindow();

    uint64_t getLeftPageSize() const;

    uint64_t getRightPageSize() const;

  private:
    const uint64_t leftPageSize;
    const uint64_t rightPageSize;
};

/**
 * @brief Proxy function for returning the pointer to the correct PagedVector
 * @return void*
 */
void* getNLJPagedVectorProxy(void* ptrNljWindow, uint64_t workerId, bool isLeftSide);

uint64_t getNLJWindowStartProxy(void* ptrNljWindow);

uint64_t getNLJWindowEndProxy(void* ptrNljWindow);

}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_NLJOPERATORHANDLER_HPP_
