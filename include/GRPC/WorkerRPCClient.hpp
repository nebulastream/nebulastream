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

#ifndef NES_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_
#define NES_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_

#include <WorkerRPCService.grpc.pb.h>
#include <WorkerRPCService.pb.h>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <Plans/Query/QueryId.hpp>
#include <grpcpp/grpcpp.h>
#include <string>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

namespace NES {

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class MonitoringPlan;

class MonitoringPlan;
typedef std::shared_ptr<MonitoringPlan> MonitoringPlanPtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class WorkerRPCClient {

  public:
    WorkerRPCClient();
    ~WorkerRPCClient();

    /**
    * @brief deploy registers and starts a query
    * @param queryId
    * @param new query plan as eto
    * @return true if succeeded, else false
    */
    bool deployQuery(std::string address, std::string executableTransferObject);

    /**
     * @brief undeploy stops and undeploy a query
     * @param queryId to undeploy
     * @return true if succeeded, else false
     */
    bool undeployQuery(std::string address, QueryId queryId);

    /**
    * @brief register a query
    * @param address: addres of node where query plan need to be registered
    * @param query plan to register
    * @return true if succeeded, else false
    */
    bool registerQuery(std::string address, QueryPlanPtr queryPlan);

    /**
     * @brief ungregisters a query
     * @param queryIdto unregister query
     * @return true if succeeded, else false
     */
    bool unregisterQuery(std::string address, QueryId queryId);

    /**
     * @brief method to start a already deployed query
     * @note if query is not deploy, false is returned
     * @param queryId to start
     * @return bool indicating success
     */
    bool startQuery(std::string address, QueryId queryId);

    /**
     * @brief method to stop a query
     * @param queryId to stop
     * @return bool indicating success
     */
    bool stopQuery(std::string address, QueryId queryId);

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @param ipAddress
     * @param the mutable schema to be extended
     * @param the buffer where the data will be written into
     * @return true if successful, else false
     */
    SchemaPtr requestMonitoringData(const std::string& address, MonitoringPlanPtr plan, NodeEngine::TupleBuffer& buf);

  private:
};
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;

}// namespace NES

#endif//NES_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_
