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
#include <Runtime/Execution/QueryStatus.hpp>
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
        std::vector<Sources::SourceHandlePtr>&& sources,
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
        std::vector<Sources::SourceHandlePtr> sources,
        std::vector<DataSinkPtr> sinks,
        std::vector<ExecutablePipelinePtr> pipelines,
        QueryManagerPtr queryManager,
        Memory::BufferManagerPtr bufferManager);
    ~ExecutableQueryPlan() override;

    void notifySourceCompletion(OriginId sourceId, QueryTerminationType terminationType);
    void notifyPipelineCompletion(ExecutablePipelinePtr pipeline, QueryTerminationType terminationType);
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

    enum class Result : uint8_t
    {
        Ok,
        Fail
    };

    /**
     * @brief returns a future that will tell us if the plan was terminated with no errors or with error.
     * @return a shared future that eventually indicates how the qep terminated
     */
    std::shared_future<Result> getTerminationFuture();

    /**
     * @brief Fail the query plan and free all associated resources.
     * @return not defined yet
     */
    bool fail();

    [[nodiscard]] QueryStatus getStatus() const;
    [[nodiscard]] const std::vector<Sources::SourceHandlePtr>& getSources() const;
    [[nodiscard]] const std::vector<DataSinkPtr>& getSinks() const;
    [[nodiscard]] const std::vector<ExecutablePipelinePtr>& getPipelines() const;
    [[nodiscard]] QueryManagerPtr getQueryManager() const;
    [[nodiscard]] Memory::BufferManagerPtr getBufferManager() const;
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
    std::vector<Sources::SourceHandlePtr> sources;
    std::vector<DataSinkPtr> sinks;
    std::vector<ExecutablePipelinePtr> pipelines;
    QueryManagerPtr queryManager;
    Memory::BufferManagerPtr bufferManager;
    std::atomic<QueryStatus> queryStatus;
    /// number of producers that provide data to this qep
    std::atomic<uint32_t> numOfTerminationTokens;
    /// promise that indicates how a qep terminates
    std::promise<Result> qepTerminationStatusPromise;
    /// future that indicates how a qep terminates
    std::future<Result> qepTerminationStatusFuture;
};

} /// namespace NES::Runtime::Execution
