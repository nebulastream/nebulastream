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

#ifndef NES_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_
#define NES_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_

#include <Plans/Query/QueryId.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <WorkerRPCService.pb.h>
#include <grpcpp/grpcpp.h>
#include <map>
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

using GrpcChannelPtr = std::shared_ptr<::grpc::Channel>;

enum RpcClientModes { Register, Unregister, Start, Stop };

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

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

    /**
     * Create shred pointer for the worker rpc
     * @return pointer to the worker rpc
     */
    static WorkerRPCClientPtr create();

    /**
        * @brief register a query
        * @param address: address of node where query plan need to be registered
        * @param query plan to register
        * @return true if succeeded, else false
        */
    bool registerQuery(const std::string& address, const QueryPlanPtr& queryPlan);

    /**
    * @brief register a query asynchronously
    * @param address: address of node where query plan need to be registered
    * @param query plan to register
    * @return true if succeeded, else false
    */
    bool registerQueryAsync(const std::string& address, const QueryPlanPtr& queryPlan, const CompletionQueuePtr& completionQueue);

    /**
     * @brief ungregisters a query
     * @param queryId to unregister query
     * @return true if succeeded, else false
     */
    bool unregisterQuery(const std::string& address, QueryId queryId);

    /**
     * @brief ungregisters a query asynchronously
     * @param queryId to unregister query
     * @return true if succeeded, else false
     */
    bool unregisterQueryAsync(const std::string& address, QueryId queryId, const CompletionQueuePtr& cq);

    /**
     * @brief method to start a already deployed query
     * @note if query is not deploy, false is returned
     * @param queryId to start
     * @return bool indicating success
     */
    bool startQuery(const std::string& address, QueryId queryId);

    /**
      * @brief method to start a already deployed query asynchronously
      * @note if query is not deploy, false is returned
      * @param queryId to start
      * @return bool indicating success
      */
    bool startQueryAsyn(const std::string& address, QueryId queryId, const CompletionQueuePtr& completionQueue);

    /**
     * @brief method to stop a query
     * @param queryId to stop
     * @return bool indicating success
     */
    bool stopQuery(const std::string& address, QueryId queryId);

    /**
     * @brief method to stop a query asynchronously
     * @param queryId to stop
     * @return bool indicating success
     */
    bool stopQueryAsync(const std::string& address, QueryId queryId, const CompletionQueuePtr& completionQueue);

    /**
     * @brief Registers to a remote worker node its monitoring plan.
     * @param address
     * @param plan the monitoring plan
     * @return bool if successful
     */
    bool registerMonitoringPlan(const std::string& address, const MonitoringPlanPtr& plan);

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @param address: the address of the worker node
     * @param tupleBuffer the buffer where the data will be written into
     * @return true if successful, else false
     */
    bool requestMonitoringData(const std::string& address, Runtime::TupleBuffer& tupleBuffer, uint64_t schemaSizeBytes);

    /**
     * @brief Requests remote worker to start buffering data on a single NetworkSink identified by
     * a query sub plan Id and a global sinkId.
     * Once buffering starts, the Network Sink no longer sends data downstream
     * @param ipAddress
     * @param querySubPlanId : the id of the query sub plan to which the Network Sink belongs
     * @param uniqueNetworkSinDescriptorId : unique id of the network sink descriptor. Used to find the Network Sink to buffer data on.
     * @return true if successful, else false
     */
    bool bufferData(const std::string& address, uint64_t querySubPlanId, uint64_t uniqueNetworkSinDescriptorId);

    /**
     * @brief requests a remote worker to reconfigure a NetworkSink so that the NetworkSink changes where it sends data to (changes downstream node)
     * @param ipAddress
     * @param newNodeId : the id of the node that the Network Sink will send data to after reconfiguration
     * @param newHostname : the hostname of the node that the NetworkSink should send data to
     * @param newPort : the port of the node that the NetworkSink should send data to
     * @param querySubPlanId : the id of the query sub plan to which the Network Sink belongs
     * @param uniqueNetworkSinDescriptorId : unique id of the network sink descriptor. Used to find the Network Sink to buffer data on.
     * @return true if successful, else false
     */
    bool updateNetworkSink(const std::string& address,
                           uint64_t newNodeId,
                           const std::string& newHostname,
                           uint32_t newPort,
                           uint64_t querySubPlanId,
                           uint64_t uniqueNetworkSinDescriptorId);

    /**
     * @brief This functions loops over all queues and wait for the async calls return
     * @param queues
     * @param mode
     * @return true if all calls returned
     */
    bool checkAsyncResult(const std::map<CompletionQueuePtr, uint64_t>& queues, RpcClientModes mode);

    /**
     * @brief method to propagate new epoch timestamp to source
     * @param timestamp: max timestamp of current epoch
     * @param queryId: query id which sources belong to
     * @param address: ip address of the source
     * @return bool indicating success
     */
    bool injectEpochBarrier(uint64_t timestamp, uint64_t queryId, const std::string& address);

  private:
    WorkerRPCClient() = default;

    /**
     * Get a grpc channel for input address
     * @param address : the address of the node
     * @return shared pointer to the channel
     */
    GrpcChannelPtr getChannel(const std::string& address);

    std::map<std::string, GrpcChannelPtr> channelCache;
};
}// namespace NES

#endif// NES_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_
