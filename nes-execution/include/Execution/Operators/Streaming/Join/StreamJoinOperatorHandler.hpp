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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_HPP_
#include <API/Schema.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Execution/Operators/Streaming/SliceAssigner.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Common.hpp>
#include <folly/Synchronized.h>
#include <list>

namespace NES::Runtime::Execution::Operators {

class StreamJoinOperatorHandler;
using StreamJoinOperatorHandlerPtr = std::shared_ptr<StreamJoinOperatorHandler>;

using WLockedSlices = folly::Synchronized<std::list<StreamSlicePtr>>::WLockedPtr;
using RLockedSlices = folly::Synchronized<std::list<StreamSlicePtr>>::RLockedPtr;

using WLockedWindows = folly::Synchronized<std::map<WindowInfo, SlicesAndState>>::WLockedPtr;

/**
 * @brief This operator is the general join operator handler and implements the JoinOperatorHandlerInterface. It is expected that
 * all StreamJoinOperatorHandlers inherit from this
 */
class StreamJoinOperatorHandler : public virtual OperatorHandler {
  public:
    static constexpr uint64_t DEFAULT_JOIN_DEPLOYMENT_TIME = 0;
    /**
     * @brief Constructor for a StreamJoinOperatorHandler
     * @param inputOrigins
     * @param outputOriginId
     * @param windowSize
     * @param windowSlide
     * @param leftSchema
     * @param rightSchema
     * @param deploymentTimes In case this join is not shared deployment times should only contain one (possibly dummy)
     * QueryId with a deployment time of 0. Otherwise, all QueryIds with their deploymentTimes.
     */
    StreamJoinOperatorHandler(const std::vector<OriginId>& inputOrigins,
                              const OriginId outputOriginId,
                              const uint64_t windowSize,
                              const uint64_t windowSlide,
                              const SchemaPtr& leftSchema,
                              const SchemaPtr& rightSchema,
                              TimeFunctionPtr leftTimeFunctionPtr,
                              TimeFunctionPtr rightTimeFunctionPtr,
                              std::map<QueryId, uint64_t> deploymentTimes);

    ~StreamJoinOperatorHandler() override = default;

    void start(PipelineExecutionContextPtr pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;

    void recreate() override;

    bool shouldRecreate() override;

    /**
     * @brief Recreate state and window info from the file
     */
    void recreateOperatorHandlerFromFile();

    /**
     * @brief Retrieves state, window info and watermarks information from max(last watermark, migrated tmstmp)
     * @return vector of tuple buffers
     */
    std::vector<Runtime::TupleBuffer> serializeOperatorHandlerForMigration() override;

    std::vector<Runtime::TupleBuffer> getSerializedPortion(uint64_t id) override;

    /**
     * @brief Retrieve the state as a vector of tuple buffers
     * Format of buffers looks like:
     * start buffers contain metadata in format:
     * -----------------------------------------
     * number of metadata buffers | number of slices (n) | number of buffers in 1st slice (m_0) | ... | number of slices in n-th slice (m_n)
     * uint64_t | uint64_t | uint64_t | ... | uint64_t
     * -----------------------------------------
     * all other buffers are: 1st buffer of 1st slice | .... | m_0 buffer of 1 slice | ... | 1 buffer of n-th slice | m_n buffer of n-th slice
     * @param startTS as a left border of slices
     * @param stopTS as a right border of slice
     * @return vector of tuple buffers
     */
    std::vector<Runtime::TupleBuffer> getStateToMigrate(uint64_t startTS, uint64_t stopTS) override;

    /**
     * @brief Retrieve window info as a vector of tuple buffers
     * Format of data buffers:
     * -----------------------------------------
     * number of windows (n) | start ts of 0 window |  end ts of 0 window | windowInfoState of 0 window | ... | start ts of n-th window |  end ts of n-th window | windowInfoState of n-th window
     * uint64_t | uint64_t | uint64_t | uint64_t | ....
     * -----------------------------------------
     * @return vector of tuple buffers
     */
    std::vector<Runtime::TupleBuffer> getWindowInfoToMigrate();

    /**
     * @brief Restores window info from vector of tuple buffers
     * Expected format of data buffers:
     * -----------------------------------------
     * number of windows (n) | start ts of 0 window |  end ts of 0 window | windowInfoState of 0 window | ... | start ts of n-th window |  end ts of n-th window | windowInfoState of n-th window
     * uint64_t | uint64_t | uint64_t | uint64_t | ....
     * -----------------------------------------
     * @param buffers with the data for recreation
     */
    void restoreWindowInfo(std::vector<Runtime::TupleBuffer>& buffers);

    /**
     * @brief Restores the state from vector of tuple buffers
     * Expected format of buffers:
     * start buffers contain metadata in format:
     * -----------------------------------------
     * number of slices (n) | number of buffers in 1st slice (m_0) | ... | number of slices in n-th slice (m_n)
     * uint64_t | uint64_t | uint64_t | ... | uint64_t
     *-----------------------------------------
     * all other buffers are: 1st buffer of 1st slice | .... | m_0 buffer of 1 slice | ... | 1 buffer of n-th slice | m_n buffer of n-th slice
     * @param buffers with the state
     */
    void restoreState(std::vector<Runtime::TupleBuffer>& buffers) override;

    /**
     * @brief read numberOfBuffers amount of buffers from file
     * @param numberOfBuffers number of buffers to read
     * @return read buffers
     */
    std::vector<TupleBuffer> readBuffers(std::ifstream& stream, uint64_t numberOfBuffers);

    /**
     * @brief Retrieve watermarks and sequence numbers as a vector of tuple buffers
     * Format of buffers looks like:
     * buffers contain metadata in format:
     * -----------------------------------------
     * number of build origins (n) | number of probe origins (m)
     * | watermark of 1st origin (i_0) | ... | watermark of n-th slice (i_n) | seq number of 1st origin (i_0) | ... | seq number of m-th slice (i_m)
     * uint64_t | uint64_t | uint64_t | ... | uint64_t
     */
    std::vector<TupleBuffer> getWatermarksToMigrate();

    /**
     * @brief Retrieves buffers from the file and restores the state
     * @param stream with the state data
     */
    void restoreStateFromFile(std::ifstream& stream) override;

    /**
     * @brief Retrieves the slice/window by a slice/window identifier. If no slice/window exists for the windowIdentifier,
     * the optional return value is of nullopt.
     * @param sliceIdentifier
     * @return Optional
     */
    std::optional<StreamSlicePtr> getSliceBySliceIdentifier(uint64_t sliceIdentifier);

    /**
     * @brief Retrieves the slice/window by its start and end time. Slices become un-writable to if they were emitted to probe
     * (happens in a shared join where one query stops) in this case such a slice is not returned.
     * If no slice that is writable exists for these times, the optional return value is of nullopt.
     * If we want to use a slice that is un-writable we need to get it by the identifier method.
     * @param slicesLocked all slices passed with a write lock
     * @param sliceStart the start time of the slice
     * @param sliceEnd the end time of the slice
     * @return Optional
     */
    std::optional<StreamSlicePtr> getSliceByStartEnd(const WLockedSlices& slicesLocked, uint64_t sliceStart, uint64_t sliceEnd);

    /**
     * @brief Triggers all slices/windows that have not been already emitted to the probe
     */
    void triggerAllSlices(PipelineExecutionContext* pipelineCtx);

    /**
     * @brief Deletes all slices/windows
     */
    void deleteAllSlices();

    /**
     * @brief Triggers windows that are ready. This method updates the watermarkProcessor and should be thread-safe
     * @param bufferMetaData
     * @param pipelineCtx
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

    virtual std::vector<WindowInfo> getAllWindowsOfDeploymentTimeForSlice(StreamSlice& slice, uint64_t deploymentTime) = 0;

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
     * @brief Creates a new slice/window for the given start, end and id
     * @param sliceStart the start time of this slice (inclusive)
     * @param sliceEnd the end time of this slice (exclusive)
     * @param sliceId the slice id (unique identifier that increments by one for each slice)
     * @return StreamSlicePtr
     */
    virtual StreamSlicePtr createNewSlice(uint64_t sliceStart, uint64_t sliceEnd, uint64_t sliceId) = 0;

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
     * @param workerThreadId
     */
    void updateWatermarkForWorker(uint64_t watermark, WorkerThreadId workerThreadId);

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

    void setBufferManager(const BufferManagerPtr& bufManager);

    /**
     * In case this joinOperatorHandler is shared by multiple queries, each query will be stored with its QueryId and
     * the time that it was deployed
     * @return QueryIds with their corresponding deployment time
     */
    std::map<QueryId, uint64_t> getQueriesAndDeploymentTimes();

    /**
     * later this handler should be adjusted while a query is running (or paused) so we would not need to store it anymore and would remove this flag
     * @param reuse true if we want to reuse this opHandler, false otherwise
     */
    void setThisForReuse(bool reuse) { setForReuse = reuse; }

    /**
     * the operator handler keeps the highest slice id. Each time a new slice is created this method needs to be called to assign
     * the new id to the slice. This method increments the highest slice id by one and returns it
     * @return the next valid slice id
     */
    [[nodiscard]] uint64_t getNextSliceId();

    /**
     * Set this to have a helper that will chose code functionality written for #5113
     * @param approach specifies which approach to use
     */
    void setApproach(SharedJoinApproach approach);
    /**
     * get helper that will chose code written for #5113
     * @return the approach that was set
     */
    SharedJoinApproach getApproach();
    /**
     * set file with the state and recreation flag
     */
    void setRecreationFileName(std::string filePath);
    /**
     * get recreation file name
     */
    std::optional<std::string> getRecreationFileName();
    bool migrating = false;

  private:
    /**
     * @brief Restores watermarks and sequence numbers in build and probe watermark processors
     * Format of buffers looks like:
     * buffers contain metadata in format:
     * -----------------------------------------
     * number of build buffers (n) | number of probe buffers (m)
     * watermark of 1st origin (i_0) | ... | watermark of n-th slice (i_n) | seq number of 1st origin (i_0) | ... | seq number of m-th slice (i_m)
     * uint64_t | uint64_t | uint64_t | ... | uint64_t
     */
    void restoreWatermarks(std::vector<Runtime::TupleBuffer>& buffers);

    /**
     * Deserialize slice from span of buffers, which is join specific and is implemented in sub-classes
     * @param buffers as a span
     * @return recreated StreamSlicePtr
     */
    virtual StreamSlicePtr deserializeSlice(std::span<const Runtime::TupleBuffer> buffers) = 0;
  protected:
    uint64_t numberOfWorkerThreads = 1;
    folly::Synchronized<std::list<StreamSlicePtr>> slices;
    uint64_t lastMigratedSeqNumber = 0;
    uint64_t windowSize;
    uint64_t windowSlide;
    folly::Synchronized<std::map<WindowInfo, SlicesAndState>> windowToSlices;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessorBuild;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessorProbe;
    std::unordered_map<WorkerThreadId, uint64_t> workerThreadIdToWatermarkMap;
    const OriginId outputOriginId;
    std::atomic<uint64_t> sequenceNumber;
    std::atomic<bool> alreadySetup{false};
    std::atomic<bool> shouldBeRecreated{false};
    std::optional<std::string> recreationFilePath;
    // TODO with issue #4517 we can remove the sizes
    size_t sizeOfRecordLeft;
    size_t sizeOfRecordRight;
    SchemaPtr leftSchema;
    SchemaPtr rightSchema;
    BufferManagerPtr bufferManager;
    TimeFunctionPtr leftTimeFunctionPtr;
    TimeFunctionPtr rightTimeFunctionPtr;

    std::map<QueryId, uint64_t>
        deploymentTimes;//just one entry with "queryId and time" == 0  iff this streamJoinOperatorHandler is not shared among multiple queries. Otherwise, we store the time each query is deployed, so their windows will start from the time they are deployed.
    SliceAssigner sliceAssigner;

    uint64_t currentSliceId = 0;
    bool setForReuse = false;
    SharedJoinApproach approach = SharedJoinApproach::UNSHARED;
    std::vector<TupleBuffer> stateToTransfer{};
    std::vector<bool> asked{false, false, false, false};
};

/**
 * gets the start time of a slice. Useful for nautilus function calls
 * @param ptrNljSlice pointer to a slice
 * @return start time of the slice
 */
uint64_t getNLJSliceStartProxy(void* ptrNljSlice);

/**
 * gets the end time of a slice. Useful for nautilus function calls
 * @param ptrNljSlice pointer to a slice
 * @return end time of the slice
 */
uint64_t getNLJSliceEndProxy(void* ptrNljSlice);

}// namespace NES::Runtime::Execution::Operators

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_HPP_
