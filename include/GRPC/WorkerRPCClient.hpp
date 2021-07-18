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

#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <Plans/Query/QueryId.hpp>
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
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class MonitoringPlan;

class MonitoringPlan;
typedef std::shared_ptr<MonitoringPlan> MonitoringPlanPtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

typedef std::shared_ptr<CompletionQueue> CompletionQueuePtr;

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

    WorkerRPCClient();
    ~WorkerRPCClient();

    void AsyncCompleteRpc();

    /**
        * @brief register a query
        * @param address: address of node where query plan need to be registered
        * @param query plan to register
        * @return true if succeeded, else false
        */
    bool registerQuery(std::string address, QueryPlanPtr queryPlan);

    /**
    * @brief register a query asynchronously
    * @param address: address of node where query plan need to be registered
    * @param query plan to register
    * @return true if succeeded, else false
    */
    bool registerQueryAsync(std::string address, QueryPlanPtr queryPlan, CompletionQueuePtr cq);

    /**
     * @brief ungregisters a query
     * @param queryIdto unregister query
     * @return true if succeeded, else false
     */
    bool unregisterQuery(std::string address, QueryId queryId);

    /**
     * @brief ungregisters a query asynchronously
     * @param queryIdto unregister query
     * @return true if succeeded, else false
     */
    bool unregisterQueryAsync(std::string address, QueryId queryId, CompletionQueuePtr cq);

    /**
     * @brief method to start a already deployed query
     * @note if query is not deploy, false is returned
     * @param queryId to start
     * @return bool indicating success
     */
    bool startQuery(std::string address, QueryId queryId);

    /**
      * @brief method to start a already deployed query asynchronously
      * @note if query is not deploy, false is returned
      * @param queryId to start
      * @return bool indicating success
      */
    bool startQueryAsyn(std::string address, QueryId queryId, CompletionQueuePtr cq);

    /**
     * @brief method to stop a query
     * @param queryId to stop
     * @return bool indicating success
     */
    bool stopQuery(std::string address, QueryId queryId);

    /**
     * @brief method to stop a query asynchronously
     * @param queryId to stop
     * @return bool indicating success
     */
    bool stopQueryAsync(std::string address, QueryId queryId, CompletionQueuePtr cq);

    /**
     * @brief Registers to a remote worker node its monitoring plan.
     * @param ipAddress
     * @param the monitoring plan
     * @return bool if successful
     */
    bool registerMonitoringPlan(const std::string& address, MonitoringPlanPtr plan);

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @param ipAddress
     * @param the buffer where the data will be written into
     * @return true if successful, else false
     */
    bool requestMonitoringData(const std::string& address, NodeEngine::TupleBuffer& buf, uint64_t schemaSizeBytes);

    /**
     * @brief This functions loops over all queues and wait for the async calls return
     * @param queues
     * @param mode
     * @return true if all calls returned
     */
    bool checkAsyncResult(std::map<CompletionQueuePtr, uint64_t> queues, RpcClientModes mode);

    /**
     * @brief requests the source config of a given physical stream name
     * @param physicalStreamName name of the requested stream
     * @return the serialized source config if the stream exists
     */
    std::tuple<bool, std::string> getSourceConfig(const std::string& address, const std::string& physicalStreamName);

    /**
     * @brief tries to set the source config of the physical stream
     * @param physicalStreamName the physical stream name of the stream to set
     * @param sourceConfig the serialized source config
     * @return true if successful
     */
    std::tuple<bool, std::string>
    setSourceConfig(const std::string& address, const std::string& physicalStreamName, const std::string& sourceConfig);

  private:
};
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;

}// namespace NES

#endif//NES_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_
