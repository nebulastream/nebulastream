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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_EXECUTABLEQUERYPLAN_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_EXECUTABLEQUERYPLAN_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/ExecutableQueryPlanStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeEventListener.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Sinks/SinksForwaredRefs.hpp>
#include <Sources/SourcesForwardedRefs.hpp>
#include <folly/Synchronized.h>
#include <atomic>
#include <future>
#include <map>
#include <vector>

namespace NES::Runtime {
class ReconfigurationMessage;
}
namespace NES::Network {
class NetworkSink;
}

namespace NES {
class ReconfigurationMarker;
using ReconfigurationMarkerPtr = std::shared_ptr<ReconfigurationMarker>;
}

namespace NES::Runtime::Execution {

enum class ExecutableQueryPlanResult : uint8_t {
    /// query was completed successfully
    Ok,
    /// query failed
    Fail
};

/**
 * @brief Represents an executable plan of an particular query.
 * Each executable query plan contains a set of sources, pipelines, and sinks.
 * A valid query plan should contain at least one source and sink.
 * This class is thread-safe.
 */
class ExecutableQueryPlan : public Reconfigurable, public RuntimeEventListener {
    friend class NES::Network::NetworkSink;

  public:
    /**
     * @brief Constructor for an executable query plan.
     * @param sharedQueryId id of the overall query
     * @param decomposedQueryId id of the sub query plan
     * @param decomposedQueryVersion version of sub query plan
     * @param sources vector of data sources
     * @param sinks vector of data sinks
     * @param pipelines vector of pipelines
     * @param queryManager shared pointer to the query manager
     * @param bufferManager shared pointer to the buffer manager
     */
    explicit ExecutableQueryPlan(SharedQueryId sharedQueryId,
                                 DecomposedQueryId decomposedQueryId,
                                 DecomposedQueryPlanVersion decomposedQueryVersion,
                                 std::vector<DataSourcePtr>&& sources,
                                 std::vector<DataSinkPtr>&& sinks,
                                 std::vector<ExecutablePipelinePtr>&& pipelines,
                                 QueryManagerPtr&& queryManager,
                                 BufferManagerPtr&& bufferManager);

    /**
     * @brief Factory to create an new executable query plan.
     * @param sharedQueryId id of the overall query
     * @param decomposedQueryId id of the sub query plan
     * @param decomposedQueryVersion version of sub query plan
     * @param sources vector of data sources
     * @param sinks vector of data sinks
     * @param pipelines vector of pipelines
     * @param queryManager shared pointer to the query manager
     * @param bufferManager shared pointer to the buffer manager
     */
    static ExecutableQueryPlanPtr create(SharedQueryId sharedQueryId,
                                         DecomposedQueryId decomposedQueryId,
                                         DecomposedQueryPlanVersion decomposedQueryVersion,
                                         std::vector<DataSourcePtr> sources,
                                         std::vector<DataSinkPtr> sinks,
                                         std::vector<ExecutablePipelinePtr> pipelines,
                                         QueryManagerPtr queryManager,
                                         BufferManagerPtr bufferManager);
    ~ExecutableQueryPlan() override;

    /**
     * @brief
     * @param source
     */
    void notifySourceCompletion(DataSourcePtr source, QueryTerminationType terminationType);

    /**
     * @brief
     * @param pipeline
     */
    void notifyPipelineCompletion(ExecutablePipelinePtr pipeline, QueryTerminationType terminationType);

    /**
     * @brief
     * @param sink
     */
    void notifySinkCompletion(DataSinkPtr sink, QueryTerminationType terminationType);

    /**
     * @brief Setup the query plan, e.g., instantiate state variables.
     */
    bool setup();

    /**
     * @brief Start the query plan, e.g., start window thread, passes stateManager further to the pipeline
     * @return Success if the query plan started
     */
    bool start();

    /**
     * @brief Stop the query plan and free all associated resources.
     */
    bool stop();

    /**
     * @brief returns a future that will tell us if the plan was terminated with no errors or with error.
     * @return a shared future that eventually indicates how the qep terminated
     */
    std::shared_future<ExecutableQueryPlanResult> getTerminationFuture();

    /**
     * @brief Fail the query plan and free all associated resources.
     * @return not defined yet
     */
    bool fail();

    ExecutableQueryPlanStatus getStatus() const;

    /**
     * @brief Get data sources.
     */
    const std::vector<DataSourcePtr>& getSources() const;

    /**
     * @brief Get data sinks.
     */
    const std::vector<DataSinkPtr>& getSinks() const;

    /**
     * @brief Get pipelines.
     * @return
     */
    const std::vector<ExecutablePipelinePtr>& getPipelines() const;

    /**
     * @brief Returns a reference to the query manager
     * @return QueryManagerPtr
     */
    QueryManagerPtr getQueryManager();

    /**
     * @brief Returns a reference to the buffer manager
     * @return BufferManagerPtr
     */
    BufferManagerPtr getBufferManager();

    /**
     * @brief Get the query id
     * @return the query id
     */
    SharedQueryId getSharedQueryId() const;

    /**
     * @brief Get the query execution plan id
     * @return the query execution plan id
     */
    DecomposedQueryId getDecomposedQueryId() const;

    /**
     * @brief Get the query execution plan version
     * @return the query execution plan version
     */
    DecomposedQueryPlanVersion getDecomposedQueryVersion() const;

    /**
     * @brief final reconfigure callback called upon a reconfiguration
     * @param task the reconfig descriptor
     */
    void postReconfigurationCallback(ReconfigurationMessage& task) override;

    /**
     * @brief destroy resources allocated to the EQP. Used
     * to explicitly destroy them outside the allocated
     * destructor.
     */
    void destroy();

    /**
     * @brief increase the counter of drained sources for this plan
     */
    void increaseSourceDrainCounter(const ReconfigurationMarkerPtr& marker);

    /**
     * @brief Get the operator ids of the network sources to be reused
     * @return vector of operator ids
     */
    [[nodiscard]] std::vector<OperatorId> getSourcesToReuse() const;

    /**
     * @brief Get the amount of network sources to be reused
     * @return the amount of sources to be reused
     */
    uint64_t getSourcesToReuseCount() const;

    /**
     * @brief set the list of sources to be reused by this plan instead of starting new sources
     */
    void setSourcesToReuse(std::vector<OperatorId> sourcesToReuse);

    /**
     * @brief adds a successor plan that will reuse sources of the current plan
     */
    bool addSuccessorPlan(ExecutableQueryPlanPtr successor);

  protected:
    /**
     * @brief API method called upon receiving an event.
     * @note Add handling for different event types here.
     * @param event
     */
    void onEvent(BaseEvent& event) override;

  private:
    /**
     * @brief This method is necessary to avoid problems with the shared_from_this machinery combined with multi-inheritance
     * @tparam Derived the class type that we want to cast the shared ptr
     * @return this instance casted to the desired shared_ptr<Derived> type
     */
    template<typename Derived>
    std::shared_ptr<Derived> shared_from_base() {
        return std::static_pointer_cast<Derived>(Reconfigurable::shared_from_this());
    }

    /**
     * @brief private helper function to increase the source drain count, check if all expected drains were received
     * @return true if all expected drains were received and the remaining sources can be transferred to the successor plan
     */
    bool handleSourceDrainHelper();

    /**
     * @brief check if all expected drain events were received to that the remaining sources can be transferred to the successor
     * plan
     */
    bool allSourcesDrained();

    /*
     * @brief propagate the reconfiguration marker to the current executable successor of all sources that are still running and
     * redirect their data flow to their new executable successors. This is used to shut down the old plan and attach the
     * sources that are marked for reuse to the new plan. This method must only be called when all non-reuse sources are drained
     * @param marker the reconfiguration marker to be propagated through the old plan
     */
    void transferSourcesToSuccessor(const ReconfigurationMarkerPtr& marker);

  private:
    const SharedQueryId sharedQueryId;
    const DecomposedQueryId decomposedQueryId;
    const DecomposedQueryPlanVersion decomposedQueryVersion;
    std::vector<DataSourcePtr> sources;
    std::vector<DataSinkPtr> sinks;
    std::vector<ExecutablePipelinePtr> pipelines;
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    std::atomic<ExecutableQueryPlanStatus> qepStatus;
    /// number of producers that provide data to this qep
    std::atomic<uint32_t> numOfTerminationTokens;
    /// promise that indicates how a qep terminates
    std::promise<ExecutableQueryPlanResult> qepTerminationStatusPromise;
    /// future that indicates how a qep terminates
    std::future<ExecutableQueryPlanResult> qepTerminationStatusFuture;
    std::atomic<uint64_t> drainedSourceCount;

    folly::Synchronized<ExecutableQueryPlanPtr> successor;
    folly::Synchronized<ReconfigurationMarkerPtr> drainMarker;
    std::vector<OperatorId> sourcesToReuse;
};

}// namespace NES::Runtime::Execution

#endif// NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_EXECUTABLEQUERYPLAN_HPP_
