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

#ifndef NES_QUERYRECONFIGURATIONPHASE_HPP
#define NES_QUERYRECONFIGURATIONPHASE_HPP

#include <Plans/Query/QueryId.hpp>
#include <iostream>
#include <memory>
#include <vector>

namespace NES {

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

class WorkerRPCClient;
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;

class ExecutionNode;
typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class QueryReconfigurationPhase;
typedef std::shared_ptr<QueryReconfigurationPhase> QueryReconfigurationPhasePtr;

class TopologyNode;
typedef std::shared_ptr<TopologyNode> TopologyNodePtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

/**
 * @brief The query reconfiguration phase is responsible for reconfiguring the query plan for a query to respective worker nodes.
 * This is useful for query merging without having to undeploy and redeploy a query.
 */
class QueryReconfigurationPhase {
  public:
    /**
     * @brief Returns a smart pointer to the QueryReconfigurationPhase
     * @param globalExecutionPlan : global execution plan
     * @param workerRpcClient : rpc client to communicate with workers
     * @return shared pointer to the instance of QueryReconfigurationPhase
     */
    static QueryReconfigurationPhasePtr create(TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan,
                                               WorkerRPCClientPtr workerRpcClient, StreamCatalogPtr streamCatalog);

    /**
     * @brief method for reconfiguring a query, only sink reconfiguration supported now
     * @param queryId : the query Id of the query to be reconfigured
     * @return true if successful else false
     */
    bool execute(QueryPlanPtr queryPlan);

    ~QueryReconfigurationPhase();

  private:
    explicit QueryReconfigurationPhase(TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan,
                                       WorkerRPCClientPtr workerRpcClient, StreamCatalogPtr streamCatalog);

    TopologyNodePtr findSinkTopologyNode(QueryPlanPtr queryPlan);

    /**
     * @brief method send query to nodes for reconfiguring sinks
     * @param queryId
     * @return bool indicating success
     */
    bool reconfigureQuery(ExecutionNodePtr executionNode, QueryPlanPtr querySubPlan);

    TopologyPtr topology;
    GlobalExecutionPlanPtr globalExecutionPlan;
    WorkerRPCClientPtr workerRPCClient;
    StreamCatalogPtr streamCatalog;
};
}// namespace NES
#endif//NES_QUERYRECONFIGURATIONPHASE_HPP
