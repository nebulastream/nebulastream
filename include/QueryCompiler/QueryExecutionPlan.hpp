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

#ifndef INCLUDE_QUERYEXECUTIONPLAN_H_
#define INCLUDE_QUERYEXECUTIONPLAN_H_
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <QueryCompiler/PipelineStage.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sources/DataSource.hpp>
#include <Windowing/WindowHandler/AbstractWindowHandler.hpp>
#include <map>

namespace NES {
class QueryManager;
typedef std::shared_ptr<QueryManager> QueryManagerPtr;

class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

/**
 * @brief A running execution plan on a node engine.
 * This class is thread-safe.
 */
class QueryExecutionPlan {
  public:
    enum QueryExecutionPlanStatus {
        Created,
        Deployed,// Created->Deployed when calling setup()
        Running, // Deployed->Running when calling start()
        Finished,
        Stopped,// Running->Stopped when calling stop() and in Running state
        ErrorState,
        Invalid
    };

  protected:
    explicit QueryExecutionPlan(QueryId queryId, QuerySubPlanId querySubPlanId, std::vector<DataSourcePtr>&& sources,
                                std::vector<DataSinkPtr>&& sinks, std::vector<PipelineStagePtr>&& stages,
                                QueryManagerPtr&& queryManager, BufferManagerPtr&& bufferManager);

  public:
    virtual ~QueryExecutionPlan();

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

    QueryExecutionPlanStatus getStatus();

    /**
     * @brief Get data sources.
     */
    std::vector<DataSourcePtr> getSources() const;

    /**
     * @brief Get data sinks.
     */
    std::vector<DataSinkPtr> getSinks() const;

    /**
     * @brief Get i-th stage.
     */
    PipelineStagePtr getStage(uint64_t index) const;

    uint64_t getStageSize() const;

    std::vector<PipelineStagePtr>& getStages();

    /**
     * @brief Gets number of pipeline stages.
     */
    uint32_t numberOfPipelineStages() { return stages.size(); }

    QueryManagerPtr getQueryManager();

    BufferManagerPtr getBufferManager();

    void print();

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
    std::vector<PipelineStagePtr> stages;
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    std::atomic<QueryExecutionPlanStatus> qepStatus;
};

}// namespace NES

#endif /* INCLUDE_QUERYEXECUTIONPLAN_H_ */
