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
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Sinks/Sink.hpp>
#include <Sources/SourceHandle.hpp>

namespace NES
{
class ReconfigurationMessage;
}
namespace NES
{

/// Each executable query plan contains a set of sources, pipelines, and sinks (but at least one source and sink). This class is thread-safe.
class ExecutableQueryPlan : public Reconfigurable
{
public:
    explicit ExecutableQueryPlan(
        QueryId queryId,
        std::vector<std::unique_ptr<Sources::SourceHandle>>&& sources,
        std::unordered_set<std::shared_ptr<NES::Sinks::Sink>>&& sinks,
        std::vector<std::shared_ptr<ExecutablePipeline>>&& pipelines,
        QueryManagerPtr&& queryManager,
        Memory::BufferManagerPtr&& bufferManager);

    static std::shared_ptr<ExecutableQueryPlan> create(
        QueryId queryId,
        std::vector<std::unique_ptr<Sources::SourceHandle>>&& sources,
        std::unordered_set<std::shared_ptr<NES::Sinks::Sink>>&& sinks,
        std::vector<std::shared_ptr<ExecutablePipeline>> pipelines,
        QueryManagerPtr queryManager,
        Memory::BufferManagerPtr bufferManager);
    ~ExecutableQueryPlan() override;

    void notifySourceCompletion(OriginId sourceId, QueryTerminationType terminationType);
    void notifyPipelineCompletion(std::shared_ptr<ExecutablePipeline> pipeline, QueryTerminationType terminationType);

    /// Setup the query plan, e.g., instantiate state variables.
    bool setup();

    /// Start the query plan, e.g., start window thread, passes stateManager further to the pipeline
    bool start();

    /// Stop the query plan and free all associated resources.
    bool stop();

    enum class Result : uint8_t
    {
        OK,
        FAILED
    };

    /// returns a future that will tell us if the plan was terminated with errors.
    std::shared_future<Result> getTerminationFuture();

    /// Fail the query plan and free all associated resources.
    bool fail();

    [[nodiscard]] QueryStatus getStatus() const;
    [[nodiscard]] const std::vector<std::unique_ptr<Sources::SourceHandle>>& getSources() const;
    [[nodiscard]] const std::unordered_set<std::shared_ptr<NES::Sinks::Sink>>& getSinks() const;
    [[nodiscard]] const std::vector<std::shared_ptr<ExecutablePipeline>>& getPipelines() const;
    [[nodiscard]] QueryManagerPtr getQueryManager() const;
    [[nodiscard]] Memory::BufferManagerPtr getBufferManager() const;
    [[nodiscard]] QueryId getQueryId() const;

    /// final reconfigure callback called upon a reconfiguration
    void postReconfigurationCallback(ReconfigurationMessage& task) override;

    /// destroy resources allocated to the EQP. Used to explicitly destroy them outside the allocated destructor.
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
    std::vector<std::unique_ptr<Sources::SourceHandle>> sources;
    std::unordered_set<std::shared_ptr<NES::Sinks::Sink>> sinks;
    std::vector<std::shared_ptr<ExecutablePipeline>> pipelines;
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

}
