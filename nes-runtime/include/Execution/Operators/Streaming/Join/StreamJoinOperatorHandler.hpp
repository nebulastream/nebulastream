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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_HPP_
#include <API/Schema.hpp>
#include <Common/Identifiers.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/Operators/Streaming/Join/StreamWindow.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Execution/Operators/Streaming/SliceAssigner.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Common.hpp>
#include <folly/Synchronized.h>
#include <list>
#include <map>
#include <optional>

namespace NES::Runtime::Execution::Operators {
/**
 * @brief This operator is the general join operator handler with basic functionality
 */
class StreamJoinOperatorHandler;
using StreamJoinOperatorHandlerPtr = std::shared_ptr<StreamJoinOperatorHandler>;

class StreamJoinOperatorHandler : public OperatorHandler {
  public:
    /**
     * @brief Constructor for a StreamJoinOperatorHandler
     * @param inputOrigins
     * @param outputOriginId
     * @param windowSize
     * @param JoinStrategy
     */
    StreamJoinOperatorHandler(const std::vector<OriginId>& inputOrigins,
                              const OriginId outputOriginId,
                              const uint64_t windowSize,
                              const QueryCompilation::StreamJoinStrategy joinStrategy,
                              uint64_t sizeOfRecordLeft,
                              uint64_t sizeOfRecordRight);

    virtual ~StreamJoinOperatorHandler() = default;

    /**
     * @brief Starts the operator handler
     * @param pipelineExecutionContext
     * @param stateManager
     * @param localStateVariableId
     */
    virtual void
    start(PipelineExecutionContextPtr pipelineExecutionContext, StateManagerPtr stateManager, uint32_t localStateVariableId) = 0;

    /**
     * @brief Stops the operator handler
     * @param terminationType
     * @param pipelineExecutionContext
     */
    virtual void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) = 0;

    void setup(uint64_t newNumberOfWorkerThreads);

    /**
     * @brief Deletes a window
     * @param windowIdentifier
     */
    void deleteWindow(uint64_t windowIdentifier);

    /**
     * @brief Retrieves the start and end of a window
     * @param windowIdentifier
     * @return Pair<windowStart, windowEnd>
     */
    std::pair<uint64_t, uint64_t> getWindowStartEnd(uint64_t windowIdentifier);

    /**
     * @brief Checks if any window can be triggered and all triggerable window identifiers are being returned in a vector.
     * This method updates the watermarkProcessor and should be thread-safe
     * @param watermarkTs
     * @param sequenceNumber
     * @param originId
     * @return Vector<uint64_t> containing windows that can be triggered
     */
    std::vector<uint64_t> checkWindowsTrigger(const uint64_t watermarkTs, const uint64_t sequenceNumber, const OriginId originId);

    /**
     * @brief Triggers all windows that have not been already emitted to the probe
     * @return Vector<uint64_t> containing windows that have not been triggered.
     */
    std::vector<uint64_t> triggerAllWindows();

    /**
     * @brief method to trigger the finished windows
     * @param windowIdentifiersToBeTriggered
     * @param workerCtx
     * @param pipelineCtx
     */
    virtual void triggerWindows(std::vector<uint64_t>& windowIdentifiersToBeTriggered,
                                WorkerContext* workerCtx,
                                PipelineExecutionContext* pipelineCtx) = 0;

    /**
     * @brief Retrieves the window by a window identifier. If no window exists for the windowIdentifier, the optional has no value.
     * @param windowIdentifier
     * @return optional
     */
    std::optional<StreamWindowPtr> getWindowByWindowIdentifier(uint64_t windowIdentifier);

    /**
     * @brief Retrieves the window by a window timestamp. If no window exists for the timestamp, the optional has no value.
     * @param timestamp
     * @return StreamWindowPtr
     */
    StreamWindowPtr getWindowByTimestampOrCreateIt(uint64_t timestamp);

    /**
     * @brief Creates a new window that corresponds to this timestamp
     * @param timestamp
     */
    StreamWindowPtr createNewWindow(uint64_t timestamp);

    /**
     * @brief get the number of windows
     * @return
     */
    uint64_t getNumberOfWindows();

    /**
     * @brief update the watermark for a particular worker
     * @param watermark
     * @param workerId
     */
    void updateWatermarkForWorker(uint64_t watermark, uint64_t workerId);

    /**
     * @brief Get the minimal watermark among all worker
     * @return
     */
    uint64_t getMinWatermarkForWorker();

    /**
     * @brief Returns the output origin id that this handler is responsible for
     * @return OperatorId
     */
    OperatorId getOutputOriginId() const;

    /**
     * @brief Returns the next sequence number for the operator that this operator handler is responsible for
     * @return uint64_t
     */
    uint64_t getNextSequenceNumber();

  protected:
    uint64_t numberOfWorkerThreads = 1;
    folly::Synchronized<std::list<StreamWindowPtr>> windows;
    SliceAssigner sliceAssigner;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
    QueryCompilation::StreamJoinStrategy joinStrategy;
    std::atomic<bool> alreadySetup{false};
    std::map<uint64_t, uint64_t> workerIdToWatermarkMap;
    const OriginId outputOriginId;
    std::atomic<uint64_t> sequenceNumber;
    size_t sizeOfRecordLeft;
    size_t sizeOfRecordRight;
};

}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_HPP_
