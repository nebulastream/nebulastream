/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef INCLUDE_NODEENGINE_QUERYEXECUTIONPLAN_H_
#define INCLUDE_NODEENGINE_QUERYEXECUTIONPLAN_H_
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <NodeEngine/Execution/ExecutableQueryPlanStatus.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <Sinks/SinksForwaredRefs.hpp>
#include <Sources/SourcesForwaredRefs.hpp>
#include <map>
#include <atomic>
#include <vector>

namespace NES::NodeEngine::Execution {

/**
 * @brief Represents an executable plan of an particular query.
 * Each executable query plan contains of at least one pipeline.
 * This class is thread-safe.
 */
class ExecutableQueryPlan {

  public:
    explicit ExecutableQueryPlan(QueryId queryId, QuerySubPlanId querySubPlanId, std::vector<DataSourcePtr>&& sources,
                                 std::vector<DataSinkPtr>&& sinks, std::vector<ExecutablePipelinePtr>&& pipelines,
                                 QueryManagerPtr&& queryManager, BufferManagerPtr&& bufferManager);

    ~ExecutableQueryPlan();

    /**
     * @brief Setup the query plan, e.g., instantiate state variables.
     */
    bool setup();

    /**
     * @brief Start the query plan, e.g., start window thread.
     */
    bool start();

    /**
     * @brief Stop the query plan and free all associated resources.
     */
    bool stop();

    ExecutableQueryPlanStatus getStatus();

    /**
     * @brief Get data sources.
     */
    std::vector<DataSourcePtr> getSources() const;

    /**
     * @brief Get data sinks.
     */
    std::vector<DataSinkPtr> getSinks() const;

    /**
     * @brief Get i-th pipeline.
     */
    ExecutablePipelinePtr getPipeline(uint64_t index) const;

    /**
     * @brief Get pipelines.
     * @return
     */
    std::vector<ExecutablePipelinePtr> getPipelines();

    /**
     * @brief Gets number of pipeline stages.
     */
    uint32_t getNumberOfPipelines();

    QueryManagerPtr getQueryManager();

    BufferManagerPtr getBufferManager();

    /**
     * @brief Get the query id
     * @return the query id
     */
    QueryId getQueryId();

    /**
     * @brief Get the query execution plan id
     * @return the query execution plan id
     */
    QuerySubPlanId getQuerySubPlanId() const;

  protected:
    const QueryId queryId;
    const QuerySubPlanId querySubPlanId;
    std::vector<DataSourcePtr> sources;
    std::vector<DataSinkPtr> sinks;
    std::vector<ExecutablePipelinePtr> pipelines;
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    std::atomic<ExecutableQueryPlanStatus> qepStatus;
};

}// namespace NES

#endif /* INCLUDE_NODEENGINE_QUERYEXECUTIONPLAN_H_ */
