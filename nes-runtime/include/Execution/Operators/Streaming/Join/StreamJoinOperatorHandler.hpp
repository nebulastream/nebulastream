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
#include <Execution/Operators/Streaming/Join/StreamSlice.hpp>
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
 * @brief This task models the information for a join window trigger, so what left and right slice identifier to join together
 */
struct EmittedNLJWindowTriggerTask {
    uint64_t leftSliceIdentifier;
    uint64_t rightSliceIdentifier;
    WindowInfo windowInfo;
};

/**
 * @brief This operator is the general join operator handler with basic functionality
 */
class StreamJoinOperatorHandler : public OperatorHandler {
  public:

    class WindowSliceIdKey {
      public:
        explicit WindowSliceIdKey(uint64_t sliceId, uint64_t windowId) : sliceId(sliceId), windowId(windowId) {}

        bool operator<(const WindowSliceIdKey& other) const {
            // For now, this should be fine as the sliceId is monotonically increasing
            if (sliceId != other.sliceId) {
                return sliceId < other.sliceId;
            } else {
                return windowId < other.windowId;
            }
        }

        uint64_t sliceId;
        uint64_t windowId;
    };

    /**
     * @brief Constructor for a StreamJoinOperatorHandler
     * @param inputOrigins
     * @param outputOriginId
     * @param sliceSize
     * @param sliceSlide
     * @param JoinStrategy
     */
    StreamJoinOperatorHandler(const std::vector<OriginId>& inputOrigins,
                              const OriginId outputOriginId,
                              const uint64_t sliceSize,
                              const uint64_t sliceSlide,
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
     * @param sliceIdentifier
     */
    void deleteSlice(uint64_t sliceIdentifier);

    /**
     * @brief Checks if any window can be triggered and all triggerable slice identifiers are being returned in a vector.
     * This method updates the watermarkProcessor and should be thread-safe
     * @param watermarkTs
     * @param sequenceNumber
     * @param originId
     * @return TriggerableSlices containing the slice ids of windows that can be triggered
     */
    TriggerableWindows checkSlicesTrigger(const uint64_t watermarkTs, const uint64_t sequenceNumber, const OriginId originId);

    /**
     * @brief Retrieves all window identifiers that correspond to this slice
     * @param slice
     * @return Vector<WindowInfo>
     */
    std::vector<WindowInfo> getAllWindowsForSlice(StreamSlice& slice);

    /**
     * @brief Returns the left and right slices for a given window, so that all tuples are being joined together
     * @param windowId
     * @return Vector<Pair<LeftSlice, RightSlice>>
     */
    std::vector<std::pair<StreamSlicePtr, StreamSlicePtr>> getSlicesLeftRightForWindow(uint64_t windowId);

    /**
     * @brief Triggers all windows that have not been already emitted to the probe
     * @return TriggerableSlices containing windows that have not been triggered.
     */
    TriggerableWindows triggerAllSlices();

    /**
     * @brief Updates the corresponding watermark processor and then deletes all slices that are not valid anymore.
     * @param waterMarkTs
     * @param sequenceNumber
     * @param originId
     */
    void deleteSlices(uint64_t waterMarkTs, uint64_t sequenceNumber, OriginId originId);

    /**
     * @brief method to trigger the finished windows
     * @param windowIdentifiersToBeTriggered
     * @param pipelineCtx
     */
    virtual void triggerSlices(TriggerableWindows& windowIdentifiersToBeTriggered,
                                PipelineExecutionContext* pipelineCtx) = 0;

    /**
     * @brief Retrieves the window by a window identifier. If no window exists for the windowIdentifier, the optional has no value.
     * @param sliceIdentifier
     * @return optional
     */
    std::optional<StreamSlicePtr> getSliceBySliceIdentifier(uint64_t sliceIdentifier);

    /**
     * @brief Retrieves the window by a window timestamp. If no window exists for the timestamp, the optional has no value.
     * @param timestamp
     * @return StreamWindowPtr
     */
    StreamSlicePtr getSliceByTimestampOrCreateIt(uint64_t timestamp);

    /**
     * @brief get the number of windows
     * @return
     */
    uint64_t getNumberOfSlices();

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
    folly::Synchronized<std::list<StreamSlicePtr>> slices;
    SliceAssigner sliceAssigner;
    folly::Synchronized<std::map<WindowSliceIdKey, StreamSlice::SliceState>> windowSliceIdToState;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessorBuild;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessorProbe;
    std::unordered_map<uint64_t, uint64_t> workerIdToWatermarkMap;
    const OriginId outputOriginId;
    std::atomic<uint64_t> sequenceNumber;
    QueryCompilation::StreamJoinStrategy joinStrategy;
    std::atomic<bool> alreadySetup{false};
    size_t sizeOfRecordLeft;
    size_t sizeOfRecordRight;
};

/**
 * @brief Deletes all slices that are not valid anymore
 * @param ptrOpHandler
 * @param watermarkTs
 * @param sequenceNumber
 * @param originId
 */
void deleteAllSlicesProxy(void* ptrOpHandler, uint64_t watermarkTs, uint64_t sequenceNumber, OriginId originId);

/*
 * @brief Sets the number of worker threads for this pipeline
 * @param ptrOpHandler
 * @param ptrPipelineContext
 */
void setNumberOfWorkerThreadsProxy(void* ptrOpHandler, void* ptrPipelineContext);


}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_HPP_
