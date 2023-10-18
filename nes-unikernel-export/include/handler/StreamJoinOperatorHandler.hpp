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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_HPP_
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Identifiers.hpp>
#include <Util/Common.hpp>
#include <handler/JoinOperatorHandlerInterfaceSlicing.hpp>

namespace NES::Runtime::Execution::Operators {

class BufferMetaData;
class WindowInfo;

/**
 * @brief This operator is the general join operator handler and implements the JoinOperatorHandlerInterface. It is expected that
 * all StreamJoinOperatorHandlers inherit from this
 */
class StreamJoinOperatorHandler : public virtual OperatorHandler {
  public:
    /**
     * @brief Constructor for a StreamJoinOperatorHandler
     * @param inputOrigins
     * @param outputOriginId
     * @param windowSize
     * @param windowSlide
     * @param sizeOfRecordLeft
     * @param sizeOfRecordRight
     */
    StreamJoinOperatorHandler(const std::vector<OriginId>& inputOrigins,
                              const OriginId outputOriginId,
                              const uint64_t windowSize,
                              const uint64_t windowSlide,
                              uint64_t sizeOfRecordLeft,
                              uint64_t sizeOfRecordRight);

    ~StreamJoinOperatorHandler() override = default;

    virtual void start(PipelineExecutionContextPtr pipelineExecutionContext,
               uint32_t localStateVariableId) override;
    virtual void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;


    /**
     * @brief Triggers all slices/windows that have not been already emitted to the probe
     */
    void triggerAllSlices(PipelineExecutionContext* pipelineCtx);

    /**
     * @brief Triggers windows that are ready. This method updates the watermarkProcessor and should be thread-safe
     * @param bufferMetaData
     */
    void checkAndTriggerWindows(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx);

    /**
     * @brief Updates the corresponding watermark processor and then deletes all slices/windows that are not valid anymore.
     * @param bufferMetaData
     */
    void deleteSlices(const BufferMetaData& bufferMetaData);

    /**
     * @brief Retrieves all window identifiers that correspond to this slice. For bucketing, this will just return one item
     * @param slice
     * @return Vector<WindowInfo>
     */
    virtual std::vector<WindowInfo> getAllWindowsForSlice(StreamSlice& slice) = 0;

    /**
     * @brief Get the number of current active slices/windows
     * @return uint64_t
     */
    uint64_t getNumberOfSlices();

    /**
     * @brief Returns the number of tuples in the slice/window for the given sliceIdentifier
     * @param sliceIdentifier
     * @param buildSide
     * @return number of tuples as uint64_t or -1 if no slice can be found for the sliceIdentifier
     */
    uint64_t getNumberOfTuplesInSlice(uint64_t sliceIdentifier, QueryCompilation::JoinBuildSideType buildSide);

    /**
     * @brief Creates a new slice/window for the given start and end
     * @param sliceStart
     * @param sliceEnd
     * @return StreamSlicePtr
     */
    virtual StreamSlicePtr createNewSlice(uint64_t sliceStart, uint64_t sliceEnd) = 0;

    /**
     * @brief Emits the left and right slice to the probe
     * @param sliceLeft
     * @param sliceRight
     * @param windowInfo
     * @param pipelineCtx
     */
    virtual void emitSliceIdsToProbe(StreamSlice& sliceLeft,
                                     StreamSlice& sliceRight,
                                     const WindowInfo& windowInfo,
                                     PipelineExecutionContext* pipelineCtx) = 0;

    /**
     * @brief Returns the output origin id that this handler is responsible for
     * @return OriginId
     */
    [[nodiscard]] OriginId getOutputOriginId() const;

    /**
     * @brief Returns the next sequence number for the operator that this operator handler is responsible for
     * @return uint64_t
     */
    uint64_t getNextSequenceNumber();

    /**
     * @brief Sets the number of worker threads for this operator handler
     * @param numberOfWorkerThreads
     */
    virtual void setNumberOfWorkerThreads(uint64_t numberOfWorkerThreads);

    /**
     * @brief update the watermark for a particular worker
     * @param watermark
     * @param workerId
     */
    void updateWatermarkForWorker(uint64_t watermark, uint64_t workerId);

    /**
     * @brief Get the minimal watermark among all worker
     * @return uint64_t
     */
    uint64_t getMinWatermarkForWorker();

    /**
     * @brief Returns the window slide
     * @return uint64_t
     */
    uint64_t getWindowSlide() const;

    /**
     * @brief Returns the window size
     * @return uint64_t
     */
    uint64_t getWindowSize() const;
};
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_HPP_
