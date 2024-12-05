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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERVALJOIN_IJOPERATORHANDLER_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERVALJOIN_IJOPERATORHANDLER_HPP_

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJInterval.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSized.hpp>

namespace NES::Runtime::Execution::Operators {

class IJOperatorHandler;
using IJOperatorHandlerPtr = std::shared_ptr<IJOperatorHandler>;
/**
 * @brief This task models the information for a trigger
 */
struct EmittedIJTriggerTask {
    uint64_t intervalIdentifier;
};

class IJOperatorHandler : public virtual OperatorHandler {
  public:
    IJOperatorHandler(const std::vector<OriginId>& inputOrigins,
                      const OriginId outputOriginId,
                      const int64_t lowerBound,
                      const int64_t upperBound,
                      const SchemaPtr& leftSchema,
                      const SchemaPtr& rightSchema,
                      const uint64_t leftPageSize,
                      const uint64_t rightPageSize);

    /**
     * @brief Creates a shared pointer on a new IJOperatorHandler object
     */
    static IJOperatorHandlerPtr create(const std::vector<OriginId>& inputOrigins,
                                       const OriginId outputOriginId,
                                       const int64_t lowerBound,
                                       const int64_t upperBound,
                                       const SchemaPtr& leftSchema,
                                       const SchemaPtr& rightSchema,
                                       const uint64_t pageSizeLeft,
                                       const uint64_t pageSizeRight);

    ~IJOperatorHandler() override = default;

    /**
     * @brief Starts the operator handler.
     */
    void start(PipelineExecutionContextPtr pipelineExecutionContext, uint32_t localStateVariableId) override;

    /**
     * @brief Stops the operator handler.
     */
    void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Triggers intervals that are ready. This method updates the watermarkProcessor and requires thread-safety. 
     */
    void checkAndTriggerIntervals(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx);

    /**
     * @brief Emits an interval to the query manager after which it will be picked up by the IJProbe
     */
    void emitIntervalToProbe(auto& interval, PipelineExecutionContext* pipelineCtx);

    /**
     * @brief Triggers all intervals that have not been already emitted to the probe when Query terminates
     */
    void triggerAllIntervals(PipelineExecutionContext* pipelineCtx);

    /**
     * @brief creates a new interval
     */
    uint64_t createAndAppendNewInterval(int64_t lowerIntervalBound, int64_t upperIntervalBound);

    /**
     * @brief update the watermark for a particular worker
     */
    void updateWatermarkForWorker(uint64_t watermark, WorkerThreadId workerThreadId);

    /**
     * @brief Creates a string representation of this OperatorHandler
     */
    std::string toString();

    /**
     * @brief Deletes all intervals from the vector of the object
     */
    void deleteAllIntervals();

    /**
     * @brief This function cleans the stored right tuples in the OpHandler and updates the timeStamp (watermark) on which right tuples were filtered
     */
    void updateRightTuples(int64_t latestRightTupleBufferCleanTimeStamp);

    /**
     * @brief GETTER and SETTER
     */
    [[nodiscard]] OriginId getOutputOriginId() const;
    std::shared_ptr<IJInterval> getInterval(u_int64_t index);
    uint64_t getNumberOfCurrentIntervals();
    BufferManagerPtr getBufferManager();
    uint64_t getPageSizeRight() const;
    int64_t getUpperBound() const;
    int64_t getLowerBound() const;
    std::vector<std::unique_ptr<Nautilus::Interface::PagedVectorVarSized>>& getUpdatedRightTuples();
    SchemaPtr& getRightSchema();
    uint64_t getMinWatermarkForWorker();
    uint64_t getNextSequenceNumber();
    void setBufferManager(const BufferManagerPtr& bufManager);
    void setNumberOfWorkerThreads(uint64_t numberOfWorkerThreads);
    /**
     * @brief Returns the next smallest Interval Start Seen used to clean right Tuple buffer
     */
    int64_t getSmallestIntervalStartSeen();

    /**
     * @brief Returns the next smallest Interval Start Seen used to clean right Tuple buffer
     */
    int64_t getLatestRightTupleBufferCleanTimeStamp();

    /**
     * @brief Retrieves the pointer to paged vector for the left or right side
     * @param workerThreadId: The id of the worker, which request the PagedVectorRef
     * @return Void pointer to the pagedVector
     */
    void* getPagedVectorRefRight(WorkerThreadId workerThreadId);

    /**
     * @brief Retrieves the interval by a interval identifier. If no interval exists for the intervalIdentifier,
     * the optional return value is of nullptr.
     * @return Optional
     */
    std::optional<IJIntervalPtr> getIntervalByIntervalIdentifier(uint64_t intervalIdentifier);

  protected:
    const uint64_t pageSizeRight;
    const uint64_t pageSizeLeft;

  private:
    folly::Synchronized<std::vector<std::shared_ptr<IJInterval>>> currentIntervals;
    uint64_t numberOfWorkerThreads = 1;
    const OriginId outputOriginId;
    int64_t smallestIntervalStartEmitted = std::numeric_limits<int64_t>::max();
    int64_t latestRightTupleBufferCleanTimeStamp = std::numeric_limits<int64_t>::min();
    const int64_t lowerBound;
    const int64_t upperBound;
    size_t sizeOfRecordLeft;
    size_t sizeOfRecordRight;
    SchemaPtr leftSchema;
    SchemaPtr rightSchema;
    std::unordered_map<WorkerThreadId, uint64_t> workerThreadIdToWatermarkMap;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessorBuild;
    std::atomic<uint64_t> sequenceNumber;
    std::atomic<bool> alreadySetup{false};
    BufferManagerPtr bufferManager;
    folly::Synchronized<std::vector<std::unique_ptr<Nautilus::Interface::PagedVectorVarSized>>> rightTuples;
    std::vector<std::unique_ptr<Nautilus::Interface::PagedVectorVarSized>> updatedRightTuples;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERVALJOIN_IJOPERATORHANDLER_HPP_
