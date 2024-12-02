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
#ifndef NES_RUNTIME_INCLUDE_RUNTIME_QUERYMANAGER_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_QUERYMANAGER_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryStatusListener.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/ExecutableQueryPlanStatus.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/ReconfigurationMessage.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/Task.hpp>
#include <Sources/DataSource.hpp>
#include <Util/AtomicCounter.hpp>
#include <Util/MMapCircularBuffer.hpp>
#include <Util/ThreadBarrier.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>
#include <Util/libcuckoo/cuckoohash_map.hh>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#ifdef ENABLE_PAPI_PROFILER
#include <Runtime/Profiler/PAPIProfiler.hpp>
#endif

#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <folly/MPMCQueue.h>
#include <folly/concurrency/UnboundedQueue.h>

namespace NES {
class NesWorker;
class BasePersistentSourceProperties;

namespace Network {
class NetworkSource;
using NetworkSourcePtr = std::shared_ptr<NetworkSource>;
}// namespace Network

namespace Runtime {

class ThreadPool;
using ThreadPoolPtr = std::shared_ptr<ThreadPool>;// TODO consider moving this atomic in c++20

class AsyncTaskExecutor;
using AsyncTaskExecutorPtr = std::shared_ptr<AsyncTaskExecutor>;

class AbstractQueryManager : public NES::detail::virtual_enable_shared_from_this<AbstractQueryManager, false>,
                             public Reconfigurable {
  public:
    using inherited0 = NES::detail::virtual_enable_shared_from_this<AbstractQueryManager, false>;

    using inherited1 = Reconfigurable;
    enum class QueryManagerStatus : uint8_t { Created, Running, Stopped, Destroyed, Failed };

    AbstractQueryManager() = delete;
    AbstractQueryManager(const AbstractQueryManager&) = delete;
    AbstractQueryManager& operator=(const AbstractQueryManager&) = delete;

    /**
    * @brief
    * @param bufferManager
    */
    explicit AbstractQueryManager(std::shared_ptr<AbstractQueryStatusListener> queryStatusListener,
                                  std::vector<BufferManagerPtr> bufferManagers,
                                  WorkerId nodeEngineId,
                                  uint16_t numThreads,
                                  HardwareManagerPtr hardwareManager,
                                  uint64_t numberOfBuffersPerEpoch,
                                  std::vector<uint64_t> workerToCoreMapping = {});

    virtual ~AbstractQueryManager() NES_NOEXCEPT(false) override;

    /**
    * @brief register a query by extracting sources, windows and sink and add them to
    * respective map
    * @param executableQueryPlan to be deployed
    */
    virtual bool registerExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& executableQueryPlan);

    /**
     * @brief deregister a query by extracting sources, windows and sink and remove them
     * from respective map
     * @param executableQueryPlan to be unregistered
     * @return bool indicating if register was successful
    */
    bool unregisterExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& executableQueryPlan);

    /**
     * @brief method to start a query
     * @param qep of the query to start
     * @return bool indicating success
     */
    [[nodiscard]] bool startExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& qep);

    /**
     * @brief method to start a query
     * @param qep of the query to start
     * @param graceful stop the query gracefully or not
     * @return bool indicating success
     */
    [[nodiscard]] bool stopExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& qep,
                                               Runtime::QueryTerminationType type = Runtime::QueryTerminationType::HardStop);

    /**
    * @brief method to fail a query
    * @param qep of the query to fail
    * @return bool indicating success
    */
    bool failExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& qep);

    /**
* @brief method to inject epoch barrier
* @param epochBarrier
* @param sourceOperatorId
* @return bool indicating success
*/
    bool injectEpochBarrier(uint64_t epochBarrier, OperatorId sourceOperatorId);
    /**
     * @brief process task from task queue
     * @param bool indicating if the thread pool is still running
     * @param worker context
     * @return an execution result
     */
    virtual ExecutionResult processNextTask(bool running, WorkerContext& workerContext) = 0;

    /**
     * @brief add work to the query manager, this methods is source-driven and is called
     * for each buffer generated by the window trigger
     * @param Pointer to the tuple buffer containing the data
     * @param Pointer to the pipeline stage that will be executed next
     * @param id of the queue where to put the task (only necessary if multiple queues are used)
     */
    virtual void
    addWorkForNextPipeline(TupleBuffer& buffer, Execution::SuccessorExecutablePipeline executable, uint32_t queueId = 0) = 0;

    /**
     * This method posts a reconfig callback task
     * @param task task to call
     */
    void postReconfigurationCallback(ReconfigurationMessage& task) override;

    /**
     * This methods triggers the reconfiguration
     * @param context workercontext
     */
    void reconfigure(ReconfigurationMessage&, WorkerContext& context) override;

    /**
     * @brief retrieve the execution status of a given local query sub plan id.
     * @param decomposedQueryId : the query sub plan id with version
     * @return status of the query sub plan
     */
    Execution::ExecutableQueryPlanStatus getQepStatus(DecomposedQueryIdWithVersion decomposedQueryIdWithVersion);

    /**
     * @brief Provides the QEP object for an id
     * @param id the plan to lookup
     * @return the QEP or null, if not found
     */
    Execution::ExecutableQueryPlanPtr getQueryExecutionPlan(DecomposedQueryIdWithVersion decomposedQueryIdWithVersion) const;

    [[nodiscard]] bool canTriggerEndOfStream(DataSourcePtr source, Runtime::QueryTerminationType);

    /**
     * @brief notify all waiting threads in getWork() to wake up and finish up
     */
    virtual void poisonWorkers() = 0;

    /**
     * @brief reset query manager. After this call, it wont be possible to use the query manager.
     */
    virtual void destroy();

    /**
     * @brief method to return the query statistics
     * @param decomposedQueryId id of the particular decomposed query
     * @return
     */
    QueryStatisticsPtr getQueryStatistics(DecomposedQueryId decomposedQueryId);

    /**
     * @brief Reset statistics for the decomposed query plan
     * @param decomposedQueryId : the decomposed query plan id
     */
    void resetQueryStatistics(DecomposedQueryId decomposedQueryId);

    /**
     * Get the id of the current node
     * @return node id
     */
    WorkerId getNodeId() const;

    /**
     * @brief this methods adds a reconfiguration task on the worker queue
     * @return true if the reconfiguration task was added correctly on the worker queue
     * N.B.: this does not not mean that the reconfiguration took place but it means that it
     * was scheduled to be executed!
     * @param sharedQueryId: the local QEP to reconfigure
     * @param queryExecutionPlanId: the local sub QEP id to reconfigure
     * @param queryExecutionPlanVersion: the local sub QEP version to reconfigure
     * @param reconfigurationDescriptor: what to do
     * @param blocking: whether to block until the reconfiguration is done. Mind this parameter because it blocks!
     */
    virtual bool addReconfigurationMessage(SharedQueryId sharedQueryId,
                                           DecomposedQueryId queryExecutionPlanId,
                                           DecomposedQueryPlanVersion queryExecutionPlanVersion,
                                           const ReconfigurationMessage& reconfigurationMessage,
                                           bool blocking = false) = 0;

    /**
     * @brief Returns the numberOfBuffersPerEpoch
     * @return numberOfBuffersPerEpoch
     */
    uint64_t getNumberOfBuffersPerEpoch() const;

    /**
     * method to get the first buffer manger
     * @return first buffer manager
     */
    BufferManagerPtr getBufferManager() { return *bufferManagers.begin(); }

    /**
     * @brief propagate a reconfiguration marker to a list of executable successors shutting down a source.
     * This function should only be called on the successors of sources that are reused by a follow up plan.
     * @param marker the reconfiguration marker to be propagated
     * @param pipelineSuccessors the pipelines of the old plan to the the marker should be propagated
    */
    void propagateReconfigurationMarkerToSuccessorList(const ReconfigurationMarkerPtr& marker,
                                                       std::vector<Execution::SuccessorExecutablePipeline> pipelineSuccessors);

    /**
     * @brief updated the mapping that relates a source to an executable query plan. The reverse mapping from decomposed query
     * plan id to source id will be updated as well.
     * @param sourceid the source id to be mapped to a new decomposed plan
     * @param newPlans a vactor of decomposed executable plan ids. Currently only vectors of size 1 are supported because source
     * reuse is not yet compatible with source sharing
     */
    void updateSourceToQepMapping(OperatorId sourceid, std::vector<Execution::ExecutableQueryPlanPtr> newPlans);

  private:
    /**
     * @brief this methods adds a reconfiguration task on the worker queue
     * @return true if the reconfiguration task was added correctly on the worker queue
     * N.B.: this does not not mean that the reconfiguration took place but it means that it
     * was scheduled to be executed!
     * @param sharedQueryId: the local QEP to reconfigure
     * @param queryExecutionPlanId: the local sub QEP id to reconfigure
     * @param queryExecutionPlanVersion: the local sub QEP version to reconfigure
     * @param buffer: a tuple buffer storing the reconfiguration message
     * @param blocking: whether to block until the reconfiguration is done. Mind this parameter because it blocks!
     */
    virtual bool addReconfigurationMessage(SharedQueryId sharedQueryId,
                                           DecomposedQueryId queryExecutionPlanId,
                                           DecomposedQueryPlanVersion queryExecutionPlanVersion,
                                           TupleBuffer&& buffer,
                                           bool blocking = false) = 0;

  public:
    /**
     * @brief This method informs the QueryManager that a task has failed
     * @param pipeline the enclosed pipeline or sink
     * @param message the reason of the feature
     */
    void notifyTaskFailure(Execution::SuccessorExecutablePipeline pipeline, const std::string& message);

    /**
     * @brief This method informs the QueryManager that a source has failed
     * @param source the failed source
     * @param errorMessage the reason of the feature
     */
    void notifySourceFailure(DataSourcePtr source, const std::string errorMessage);

    /**
     * @brief Informs the query manager about a status change in a sub query plan
     * @param qep the sub query plan
     * @param newStatus the new status of the query plan
     */
    void notifyQueryStatusChange(const Execution::ExecutableQueryPlanPtr& qep, Execution::ExecutableQueryPlanStatus newStatus);

    /**
     * @brief get the shared query id mapped to the decomposed query plan id
     * @param decomposedQueryId: the decomposed query plan id with version
     * @return shared query id
     */
    SharedQueryId getSharedQueryId(DecomposedQueryIdWithVersion decomposedQueryIdWithVersion) const;

    /**
     * @brief introduces end of stream to all QEPs connected to this source
     * @param source the source
     * @param graceful hard or soft termination
     * @return true if it went through
     */
    bool addEndOfStream(DataSourcePtr source, Runtime::QueryTerminationType graceful = Runtime::QueryTerminationType::Graceful);

    /**
     * @brief Triggers an epoch propagation for all network sinks
     * @param decomposedQueryId
     * @param epochBarrier timestamp that should be trimmed in the storage
     * @return true if successful
     */
    bool sendTrimmingReconfiguration(DecomposedQueryId decomposedQueryId, uint64_t epochBarrier);

    /**
  * @brief Triggers an epoch propagation for all network sinks
  * @param decomposedQueryId
  * @param epochBarrier timestamp that should be trimmed in the storage
  * @return true if successful
  */
    bool propagateEpochBackwards(DecomposedQueryId decomposedQueryId, uint64_t epochBarrier);

    /**
     * @brief propagates a reconfiguration marker to all downstream operators of a source operator
     * @param source the source operator
     * @return true if it went through
     */
    bool propagateReconfigurationMarker(const ReconfigurationMarkerPtr& marker, DataSourcePtr source);

    /**
     * @brief starts new executable plan and propagates reconfiguration marker to it's source
     * @param marker reconfiguration marker
     * @param decomposedQueryId id and version of current plan
     * @return true if plan was started
     */
    bool startNewExecutableQueryPlanAndPropagateMarker(const ReconfigurationMarkerPtr marker,
                                                       DecomposedQueryIdWithVersion decomposedQueryIdWithVersion);

    /**
     * @return true if thread pool is running
     */
    bool isThreadPoolRunning() const;

    /**
     * @brief get number of tasks in the queue
     * @return task count
     */
    virtual uint64_t getNumberOfTasksInWorkerQueues() const = 0;

    /**
     * Return the current occupation of the task queue
     * @return number of tasks in the queue
     */
    uint64_t getCurrentTaskSum();

    /**
     * Returns the current number of worker threads
     * @return thread cnt
     */
    uint64_t getNumberOfWorkerThreads();

    /**
     * @brief Notifies that a source operator is done with its execution
     * @param source the completed source
     * @param terminationType the type of termination (e.g., failure, soft)
     */
    void notifySourceCompletion(DataSourcePtr source, QueryTerminationType terminationType);

    /**
     * @brief Notifies that a pipeline is done with its execution
     * @param decomposedQueryId the plan the pipeline belongs to
     * @param pipeline the terminated pipeline
     * @param terminationType the type of termination (e.g., failure, soft)
     */
    void notifyPipelineCompletion(DecomposedQueryId decomposedQueryId,
                                  Execution::ExecutablePipelinePtr pipeline,
                                  QueryTerminationType terminationType);

    /**
     * @brief Notifies that a sink operator is done with its execution
     * @param decomposedQueryId the id of the plan the sink belongs to
     * @param decomposedQueryVersion the version of the plan the sink belongs to
     * @param sink the terminated sink
     * @param terminationType the type of termination (e.g., failure, soft)
     */
    void notifySinkCompletion(DecomposedQueryId decomposedQueryId,
                              DecomposedQueryPlanVersion decomposedQueryVersion,
                              DataSinkPtr sink,
                              QueryTerminationType terminationType);

    /**
     * @brief update the version of a running executable query plan
     * @param idWithVersion the id of the plan and its current version
     * @param newVersion the new version to set for the plan
     * @return true if the plan was successfully updated
     */
    bool updatePlanVersion(DecomposedQueryIdWithVersion idWithVersion, DecomposedQueryPlanVersion newVersion);

    std::optional<ReconfigurationMarkerEventPtr> getReconfigurationEvent(Network::NetworkSourcePtr networkSource,
                                                                         SharedQueryId sharedQueryId,
                                                                         const ReconfigurationMarker& marker) const;
    std::unordered_set<DecomposedQueryIdWithVersion> getExecutablePlanIdsForSource(DataSourcePtr source) const;

    // Map containing persistent source properties
    folly::Synchronized<std::unordered_map<std::string, std::shared_ptr<BasePersistentSourceProperties>>>
        persistentSourceProperties;

  private:
    friend class ThreadPool;
    friend class NodeEngine;
    /**
    * @brief method to start the thread pool
    * @param nodeEngineId the id of the owning node engine
    * @param numberOfBuffersPerWorker
    * @return bool indicating success
    */
    virtual bool startThreadPool(uint64_t numberOfBuffersPerWorker) = 0;

  protected:
    /**
     * @brief finalize task execution by:
     * 1.) update statistics (number of processed tuples and tasks)
     * 2.) release input buffer (give back to the buffer manager)
     * @param reference to processed task
     * @oaram reference to worker context
     */
    void completedWork(Task& task, WorkerContext& workerContext);

    /**
     * @brief Method to update the statistics
     * @param task
     * @param sharedQueryId
     * @param decomposedQueryId
     * @param pipelineId
     * @param workerContext
     */
    virtual void updateStatistics(const Task& task,
                                  SharedQueryId sharedQueryId,
                                  DecomposedQueryId decomposedQueryId,
                                  PipelineId pipelineId,
                                  WorkerContext& workerContext);

    /**
     * @brief Executes cleaning up logic on the task queue
     * @return an ExecutionResult
     */
    virtual ExecutionResult terminateLoop(WorkerContext&) = 0;

    /**
     * @brief Triggers a soft end of stream for a source
     * @param source the source for which to trigger the soft end of stream
     * @return true if successful
     */
    bool addSoftEndOfStream(DataSourcePtr source);

    /**
     * @brief Triggers a hard end of stream for a source
     * @param source the source for which to trigger the hard end of stream
     * @return true if successful
     */
    bool addHardEndOfStream(DataSourcePtr source);

    /**
     * @brief Triggers a failure end of stream for a source
     * @param source the source for which to trigger the failure end of stream
     * @return true if successful
     */
    bool addFailureEndOfStream(DataSourcePtr source);

    /**
     * @brief Returns the next free task id
     * @return next task id
     */
    uint64_t getNextTaskId();

    /**
     * @brief mark a source as drained and schedule a reconfiguration marker to be propagated to the sources to be reused, once
     * all expected drains have been received
     * @param marker This reconfiguration marker is stored in the QEP to which the source belongs. Once all expected drain events
     * have been received, it is propgated to the old successor of all sources to be reused
     * @param sourceid the id of the source to be marked as drained
     */
    void markSourceAsDrained(const ReconfigurationMarkerPtr& marker, OperatorId sourceid);

    WorkerId nodeEngineId;
    std::atomic_uint64_t taskIdCounter = 0;
    std::vector<BufferManagerPtr> bufferManagers;

    uint16_t numThreads;

    HardwareManagerPtr hardwareManager;

    /// worker threads running compute tasks
    ThreadPoolPtr threadPool{nullptr};

    /// worker thread for async maintenance task, e.g., fail queryIdAndCatalogEntryMapping
    AsyncTaskExecutorPtr asyncTaskExecutor;

    std::unordered_map<DecomposedQueryIdWithVersion, Execution::ExecutableQueryPlanPtr> runningQEPs;

    //TODO:check if it would be better to put it in the thread context
    mutable std::mutex statisticsMutex;
    cuckoohash_map<DecomposedQueryId, QueryStatisticsPtr> queryToStatisticsMap;

    mutable std::mutex reconfigurationMutex;

    std::vector<uint64_t> workerToCoreMapping;
    mutable std::recursive_mutex queryMutex;

    std::atomic<QueryManagerStatus> queryManagerStatus{QueryManagerStatus::Created};

    std::vector<AtomicCounter<uint64_t>> tempCounterTasksCompleted;

    std::shared_ptr<AbstractQueryStatusListener> queryStatusListener;

    std::unordered_map<DecomposedQueryIdWithVersion, std::vector<OperatorId>> decomposeQueryToSourceIdMapping;

    std::unordered_map<OperatorId, std::vector<Execution::ExecutableQueryPlanPtr>> sourceToQEPMapping;

    uint64_t numberOfBuffersPerEpoch;
#ifdef ENABLE_PAPI_PROFILER
    std::vector<Profiler::PapiCpuProfilerPtr> cpuProfilers;
#endif
};

class DynamicQueryManager : public AbstractQueryManager {
  public:
    explicit DynamicQueryManager(std::shared_ptr<AbstractQueryStatusListener> queryStatusListener,
                                 std::vector<BufferManagerPtr> bufferManager,
                                 WorkerId nodeEngineId,
                                 uint16_t numThreads,
                                 HardwareManagerPtr hardwareManager,
                                 uint64_t numberOfBuffersPerEpoch,
                                 std::vector<uint64_t> workerToCoreMapping = {});

    void destroy() override;

    /**
     * @brief process task from task queue
     * @param bool indicating if the thread pool is still running
     * @param worker context
     * @return an execution result
     */
    ExecutionResult processNextTask(bool running, WorkerContext& workerContext) override;

    /**
     * @brief add work to the query manager, this methods is source-driven and is called
     * for each buffer generated by the window trigger
     * @param Pointer to the tuple buffer containing the data
     * @param Pointer to the pipeline stage that will be executed next
     * @param id of the queue where to put the task (only necessary if multiple queues are used)
     */
    void
    addWorkForNextPipeline(TupleBuffer& buffer, Execution::SuccessorExecutablePipeline executable, uint32_t queueId = 0) override;

  protected:
    /**
     * @brief
     * @return
     */
    ExecutionResult terminateLoop(WorkerContext&) override;

    /**
     * @brief
     * @param task
     * @param sharedQueryId
     * @param decomposedQueryId
     * @param pipeId
     * @param workerContext
     */
    void updateStatistics(const Task& task,
                          SharedQueryId sharedQueryId,
                          DecomposedQueryId decomposedQueryId,
                          PipelineId pipeId,
                          WorkerContext& workerContext) override;

    uint64_t getNumberOfTasksInWorkerQueues() const override;

    /**
      * @brief Returns the numberOfBuffersPerEpoch
      * @return numberOfBuffersPerEpoch
      */
    uint64_t getNumberOfBuffersPerEpoch() const;

  private:
    /**
     * @brief this methods adds a reconfiguration task on the worker queue
     * @return true if the reconfiguration task was added correctly on the worker queue
     * N.B.: this does not not mean that the reconfiguration took place but it means that it
     * was scheduled to be executed!
     * @param sharedQueryId: the local QEP to reconfigure
     * @param queryExecutionPlanId: the local sub QEP id to reconfigure
     * @param queryExecutionPlanVersion: the local sub QEP version to reconfigure
     * @param buffer: a tuple buffer storing the reconfiguration message
     * @param blocking: whether to block until the reconfiguration is done. Mind this parameter because it blocks!
     */
    bool addReconfigurationMessage(SharedQueryId sharedQueryId,
                                   DecomposedQueryId decomposedQueryId,
                                   DecomposedQueryPlanVersion queryExecutionPlanVersion,
                                   TupleBuffer&& buffer,
                                   bool blocking = false) override;

    /**
     * @brief this methods adds a reconfiguration task on the worker queue
     * @return true if the reconfiguration task was added correctly on the worker queue
     * N.B.: this does not not mean that the reconfiguration took place but it means that it
     * was scheduled to be executed!
     * @param sharedQueryId: the local QEP to reconfigure
     * @param queryExecutionPlanId: the local sub QEP id to reconfigure
     * @param queryExecutionPlanVersion: the local sub QEP version to reconfigure
     * @param reconfigurationDescriptor: what to do
     * @param blocking: whether to block until the reconfiguration is done. Mind this parameter because it blocks!
     */
    bool addReconfigurationMessage(SharedQueryId sharedQueryId,
                                   DecomposedQueryId queryExecutionPlanId,
                                   DecomposedQueryPlanVersion queryExecutionPlanVersion,
                                   const ReconfigurationMessage& reconfigurationMessage,
                                   bool blocking = false) override;

    /**
    * @brief method to start the thread pool
    * @param nodeEngineId the id of the owning node engine
    * @param numberOfBuffersPerWorker
    * @return bool indicating success
    */
    bool startThreadPool(uint64_t numberOfBuffersPerWorker) override;

    /**
     * @brief
     */
    void poisonWorkers() override;

  private:
    folly::MPMCQueue<Task> taskQueue;
};

class MultiQueueQueryManager : public AbstractQueryManager {
  public:
    explicit MultiQueueQueryManager(std::shared_ptr<AbstractQueryStatusListener> queryStatusListener,
                                    std::vector<BufferManagerPtr> bufferManager,
                                    WorkerId nodeEngineId,
                                    uint16_t numThreads,
                                    HardwareManagerPtr hardwareManager,
                                    uint64_t numberOfBuffersPerEpoch,
                                    std::vector<uint64_t> workerToCoreMapping = {},
                                    uint64_t numberOfQueues = 1,
                                    uint64_t numberOfThreadsPerQueue = 1);

    void destroy() override;

    bool registerExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& qep) override;

    /**
     * @brief process task from task queue
     * @param bool indicating if the thread pool is still running
     * @param worker context
     * @return an execution result
     */
    ExecutionResult processNextTask(bool running, WorkerContext& workerContext) override;

    /**
     * @brief add work to the query manager, this methods is source-driven and is called
     * for each buffer generated by the window trigger
     * @param Pointer to the tuple buffer containing the data
     * @param Pointer to the pipeline stage that will be executed next
     * @param id of the queue where to put the task (only necessary if multiple queues are used)
     */
    void
    addWorkForNextPipeline(TupleBuffer& buffer, Execution::SuccessorExecutablePipeline executable, uint32_t queueId = 0) override;

    uint64_t getNumberOfTasksInWorkerQueues() const override;

    /**
      * @brief Returns the numberOfBuffersPerEpoch
      * @return numberOfBuffersPerEpoch
      */
    uint64_t getNumberOfBuffersPerEpoch() const;

  protected:
    /**
     * @brief
     * @return
     */
    ExecutionResult terminateLoop(WorkerContext&) override;

    /**
     * @brief
     * @param sharedQueryId
     * @param decomposedQueryId
     * @param pipeId
     * @param workerContext
     */
    void updateStatistics(const Task& task,
                          SharedQueryId sharedQueryId,
                          DecomposedQueryId decomposedQueryId,
                          PipelineId pipeId,
                          WorkerContext& workerContext) override;

  private:
    /**
     * @brief this methods adds a reconfiguration task on the worker queue
     * @return true if the reconfiguration task was added correctly on the worker queue
     * N.B.: this does not not mean that the reconfiguration took place but it means that it
     * was scheduled to be executed!
     * @param sharedQueryId: the local QEP to reconfigure
     * @param queryExecutionPlanId: the local sub QEP id to reconfigure
     * @param queryExecutionPlanVersion: the local sub QEP version to reconfigure
     * @param buffer: a tuple buffer storing the reconfiguration message
     * @param blocking: whether to block until the reconfiguration is done. Mind this parameter because it blocks!
     */
    bool addReconfigurationMessage(SharedQueryId sharedQueryId,
                                   DecomposedQueryId queryExecutionPlanId,
                                   DecomposedQueryPlanVersion queryExecutionPlanVersion,
                                   TupleBuffer&& buffer,
                                   bool blocking = false) override;

    /**
     * @brief this methods adds a reconfiguration task on the worker queue
     * @return true if the reconfiguration task was added correctly on the worker queue
     * N.B.: this does not not mean that the reconfiguration took place but it means that it
     * was scheduled to be executed!
     * @param sharedQueryId: the local QEP to reconfigure
     * @param queryExecutionPlanId: the local sub QEP id to reconfigure
     * @param queryExecutionPlanVersion: the local sub QEP version to reconfigure
     * @param reconfigurationDescriptor: what to do
     * @param blocking: whether to block until the reconfiguration is done. Mind this parameter because it blocks!
     */
    bool addReconfigurationMessage(SharedQueryId sharedQueryId,
                                   DecomposedQueryId queryExecutionPlanId,
                                   DecomposedQueryPlanVersion queryExecutionPlanVersion,
                                   const ReconfigurationMessage& reconfigurationMessage,
                                   bool blocking = false) override;

    void poisonWorkers() override;

    /**
    * @brief method to start the thread pool
    * @param nodeEngineId the id of the owning node engine
    * @param numberOfBuffersPerWorker
    * @return bool indicating success
    */
    bool startThreadPool(uint64_t numberOfBuffersPerWorker) override;

  private:
    std::vector<folly::MPMCQueue<Task>> taskQueues;
    uint16_t numberOfQueues = 1;
    uint16_t numberOfThreadsPerQueue;
    std::unordered_map<DecomposedQueryId, uint64_t> queryToTaskQueueIdMap;
    std::atomic<uint64_t> currentTaskQueueId = 0;
};

using QueryManagerPtr = std::shared_ptr<AbstractQueryManager>;
using DynamicQueryManagerPtr = std::shared_ptr<DynamicQueryManager>;
using MultiQueueQueryManagerPtr = std::shared_ptr<MultiQueueQueryManager>;

}// namespace Runtime
}// namespace NES
#endif// NES_RUNTIME_INCLUDE_RUNTIME_QUERYMANAGER_HPP_
