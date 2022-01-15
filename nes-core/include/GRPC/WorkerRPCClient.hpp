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

#ifndef NES_INCLUDE_GRPC_WORKER_RPC_CLIENT_HPP_
#define NES_INCLUDE_GRPC_WORKER_RPC_CLIENT_HPP_

#include <Plans/Query/QueryId.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <WorkerRPCService.pb.h>
#include <grpcpp/grpcpp.h>
#include <string>
#include <thread>

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
namespace NES {

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class MonitoringPlan;

class MonitoringPlan;
using MonitoringPlanPtr = std::shared_ptr<MonitoringPlan>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

using CompletionQueuePtr = std::shared_ptr<CompletionQueue>;

enum RpcClientModes { Register, Unregister, Start, Stop };

class WorkerRPCClient {
  public:
    template<typename ReplayType>
    struct AsyncClientCall {
        // Container for the data we expect from the server.
        ReplayType reply;

        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // Storage for the status of the RPC upon completion.
        Status status;

        std::unique_ptr<ClientAsyncResponseReader<ReplayType>> responseReader;
    };

    WorkerRPCClient() = default;

    void AsyncCompleteRpc();

    /**
        * @brief register a query
        * @param address: address of node where query plan need to be registered
        * @param query plan to register
        * @return true if succeeded, else false
        */
    static bool registerQuery(const std::string& address, const QueryPlanPtr& queryPlan);

    /**
    * @brief register a query asynchronously
    * @param address: address of node where query plan need to be registered
    * @param query plan to register
    * @return true if succeeded, else false
    */
    static bool registerQueryAsync(const std::string& address, const QueryPlanPtr& queryPlan, const CompletionQueuePtr& cq);

    /**
     * @brief ungregisters a query
     * @param queryId to unregister query
     * @return true if succeeded, else false
     */
    static bool unregisterQuery(const std::string& address, QueryId queryId);

    /**
     * @brief ungregisters a query asynchronously
     * @param queryId to unregister query
     * @return true if succeeded, else false
     */
    static bool unregisterQueryAsync(const std::string& address, QueryId queryId, const CompletionQueuePtr& cq);

    /**
     * @brief method to start a already deployed query
     * @note if query is not deploy, false is returned
     * @param queryId to start
     * @return bool indicating success
     */
    static bool startQuery(const std::string& address, QueryId queryId);

    /**
      * @brief method to start a already deployed query asynchronously
      * @note if query is not deploy, false is returned
      * @param queryId to start
      * @return bool indicating success
      */
    static bool startQueryAsyn(const std::string& address, QueryId queryId, const CompletionQueuePtr& cq);

    /**
     * @brief method to stop a query
     * @param queryId to stop
     * @return bool indicating success
     */
    static bool stopQuery(const std::string& address, QueryId queryId);

    /**
     * @brief method to stop a query asynchronously
     * @param queryId to stop
     * @return bool indicating success
     */
    static bool stopQueryAsync(const std::string& address, QueryId queryId, const CompletionQueuePtr& cq);

    /**
     * @brief Registers to a remote worker node its monitoring plan.
     * @param ipAddress
     * @param the monitoring plan
     * @return bool if successful
     */
    static bool registerMonitoringPlan(const std::string& address, const MonitoringPlanPtr& plan);

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @param ipAddress
     * @param the buffer where the data will be written into
     * @return true if successful, else false
     */
    static bool requestMonitoringData(const std::string& address, Runtime::TupleBuffer& buf, uint64_t schemaSizeBytes);

    /**
     * @brief This functions loops over all queues and wait for the async calls return
     * @param queues
     * @param mode
     * @return true if all calls returned
     */
    static bool checkAsyncResult(const std::map<CompletionQueuePtr, uint64_t>& queues, RpcClientModes mode);

    static bool getDumpInfoFromNode(const std::string& address, std::string* mapAsJson);

  private:
};
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

}// namespace NES

#endif// NES_INCLUDE_GRPC_WORKER_RPC_CLIENT_HPP_
