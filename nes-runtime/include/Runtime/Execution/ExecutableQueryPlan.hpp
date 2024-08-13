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

#pragma once

#include <atomic>
#include <future>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutableQueryPlanStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Sinks/SinksForwaredRefs.hpp>
#include <Sources/SourceHandle.hpp>

namespace NES::Runtime
{
class ReconfigurationMessage;
}
namespace NES::Runtime::Execution
{

enum class ExecutableQueryPlanResult : uint8_t
{
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
class ExecutableQueryPlan : public Reconfigurable
{
public:
    /**
     * @brief Constructor for an executable query plan.
     * @param queryId id of query
     * @param sources vector of data sources
     * @param sinks vector of data sinks
     * @param pipelines vector of pipelines
     * @param queryManager shared pointer to the query manager
     * @param bufferManager shared pointer to the buffer manager
     */
    explicit ExecutableQueryPlan(
        QueryId queryId,
        std::vector<SourceHandlePtr>&& sources,
        std::vector<DataSinkPtr>&& sinks,
        std::vector<ExecutablePipelinePtr>&& pipelines,
        QueryManagerPtr&& queryManager,
        Memory::BufferManagerPtr&& bufferManager);

    /**
     * @brief Factory to create an new executable query plan.
     * @param queryId id of query
     * @param sources vector of data sources
     * @param sinks vector of data sinks
     * @param pipelines vector of pipelines
     * @param queryManager shared pointer to the query manager
     * @param bufferManager shared pointer to the buffer manager
     */
    static ExecutableQueryPlanPtr create(
        QueryId queryId,
        std::vector<SourceHandlePtr> sources,
        std::vector<DataSinkPtr> sinks,
        std::vector<ExecutablePipelinePtr> pipelines,
        QueryManagerPtr queryManager,
        Memory::BufferManagerPtr bufferManager);
    ~ExecutableQueryPlan() override;

    /**
     * @brief
     * @param source
     */
    void notifySourceCompletion(OriginId sourceId, QueryTerminationType terminationType);

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

    [[nodiscard]] ExecutableQueryPlanStatus getStatus();

    /**
     * @brief Get data sources.
     */
    [[nodiscard]] const std::vector<SourceHandlePtr>& getSources() const;

    /**
     * @brief Get data sinks.
     */
    [[nodiscard]] const std::vector<DataSinkPtr>& getSinks() const;

    /**
     * @brief Get pipelines.
     * @return
     */
    [[nodiscard]] const std::vector<ExecutablePipelinePtr>& getPipelines() const;

    /**
     * @brief Returns a reference to the query manager
     * @return QueryManagerPtr
     */
    [[nodiscard]] QueryManagerPtr getQueryManager();

    /**
     * @brief Returns a reference to the buffer manager
     * @return BufferManagerPtr
     */
    [[nodiscard]] Memory::BufferManagerPtr getBufferManager();

    /**
     * @brief Get the query id
     * @return the query id
     */
    [[nodiscard]] QueryId getQueryId() const;

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

private:
    /**
     * @brief This method is necessary to avoid problems with the shared_from_this machinery combined with multi-inheritance
     * @tparam Derived the class type that we want to cast the shared ptr
     * @return this instance casted to the desired shared_ptr<Derived> type
     */
    template <typename Derived>
    std::shared_ptr<Derived> shared_from_base()
    {
        return std::static_pointer_cast<Derived>(Reconfigurable::shared_from_this());
    }

private:
    const QueryId queryId;
    std::vector<SourceHandlePtr> sources;
    std::vector<DataSinkPtr> sinks;
    std::vector<ExecutablePipelinePtr> pipelines;
    QueryManagerPtr queryManager;
    Memory::BufferManagerPtr bufferManager;
    std::atomic<ExecutableQueryPlanStatus> qepStatus;
    /// number of producers that provide data to this qep
    std::atomic<uint32_t> numOfTerminationTokens;
    /// promise that indicates how a qep terminates
    std::promise<ExecutableQueryPlanResult> qepTerminationStatusPromise;
    /// future that indicates how a qep terminates
    std::future<ExecutableQueryPlanResult> qepTerminationStatusFuture;
};

} /// namespace NES::Runtime::Execution
