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

#ifndef NES_COORDINATOR_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_
#define NES_COORDINATOR_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_

#include <Checkpoint.pb.h>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <StatisticCollection/StatisticCache/AbstractStatisticCache.hpp>
#include <StatisticCollection/StatisticProbeHandling/AbstractStatisticProbeGenerator.hpp>
#include <Util/TimeMeasurement.hpp>
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

class ReconfigurationMarker;
using ReconfigurationMarkerPtr = std::shared_ptr<ReconfigurationMarker>;

namespace Monitoring {
class MonitoringPlan;
using MonitoringPlanPtr = std::shared_ptr<MonitoringPlan>;
}// namespace Monitoring

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

using CompletionQueuePtr = std::shared_ptr<CompletionQueue>;

namespace Spatial::DataTypes::Experimental {
class Waypoint;
}// namespace Spatial::DataTypes::Experimental

enum class RpcClientMode : uint8_t { Register, Unregister, Start, Stop, Reconfiguration, Checkpoint };

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

struct RpcAsyncRequest {
    CompletionQueuePtr completionQueue;
    RpcClientMode rpcClientMode;
};

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
     * @brief Create instance of worker rpc client
     * @return shared pointer to the worker RPC client
     */
    static WorkerRPCClientPtr create();

    /**
     * @brief register a query
     * @param address: address of node where query plan need to be registered
     * @param decomposedQueryPlan plan to register
     * @return true if succeeded, else false
     */
    bool registerDecomposedQuery(const std::string& address, const DecomposedQueryPlanPtr& decomposedQueryPlan);

    /**
     * @brief register a query asynchronously
     * @param address: address of node where query plan need to be registered
     * @param query plan to register
     * @param cq the completion queue
     */
    void registerDecomposedQueryAsync(const std::string& address,
                                      const DecomposedQueryPlanPtr& decomposedQueryPlan,
                                      const CompletionQueuePtr& cq);

    /**
     * @brief unregisters a decomposed query
     * @param sharedQueryId: id of the shared query plan to which the decomposed query plan serves
     * @param decomposedQueryId: id of the decomposed query to unregister
     * @return true if succeeded, else false
     */
    bool unregisterDecomposedQuery(const std::string& address, SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId);

    /**
     * @brief un-registers a decomposed query asynchronously
     * @param sharedQueryId: id of the shared query plan to which the decomposed query plan serves
     * @param decomposedQueryId: id of the decomposed query to unregister
     */
    void unregisterDecomposedQueryAsync(const std::string& address,
                                        SharedQueryId sharedQueryId,
                                        DecomposedQueryId decomposedQueryId,
                                        const CompletionQueuePtr& cq);

    /**
     * @brief method to start a already deployed decomposed query
     * @note if query is not deploy, false is returned
     * @param sharedQueryId: id of the shared query plan to which the decomposed query plan serves
     * @param decomposedQueryId: id of the decomposed query to start
     * @return bool indicating success
     */
    bool startDecomposedQuery(const std::string& address, SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId);

    /**
      * @brief method to start a already deployed query asynchronously
      * @note if query is not deploy, false is returned
      * @param address the address of the worker
      * @param sharedQueryId: id of the shared query plan to which the decomposed query plan serves
      * @param decomposedQueryId: id of the decomposed query to start
      * @param cq the completion queue
      */
    void startDecomposedQueryAsync(const std::string& address,
                                   SharedQueryId sharedQueryId,
                                   DecomposedQueryId decomposedQueryId,
                                   const CompletionQueuePtr& cq);

    /**
     * @brief method to stop a query
     * @param address address of the new worker
     * @param sharedQueryId to stop
     * @param sharedQueryId: id of the shared query plan to which the decomposed query plan serves
     * @param decomposedQueryId: id of the decomposed query to stop
     * @return bool indicating success
     */
    bool stopDecomposedQuery(const std::string& address,
                             SharedQueryId sharedQueryId,
                             DecomposedQueryId decomposedQueryId,
                             Runtime::QueryTerminationType terminationType);

    /**
     * @brief method to stop a query asynchronously
     * @param address : address of the worker
     * @param sharedQueryId: id of the shared query plan to which the decomposed query plan serves
     * @param decomposedQueryId: id of the decomposed query to stop
     * @param terminationType: the termination type
     * @param cq: completion queue of grpc requests
     */
    void stopDecomposedQueryAsync(const std::string& address,
                                  SharedQueryId sharedQueryId,
                                  DecomposedQueryId decomposedQueryId,
                                  Runtime::QueryTerminationType terminationType,
                                  const CompletionQueuePtr& cq);

    /**
     * @brief method to send reconfiguration marker to the worker for specific decomposed query plan
     * @param address: address of the worker
     * @param sharedQueryId: the shared query id
     * @param decomposedQueryId: the decomposed query id
     * @param reconfigurationMarker: the reconfiguration marker
     * @param cq: the completion queue of grpc server
     */
    void addReconfigurationMarker(const std::string& address,
                                  const SharedQueryId& sharedQueryId,
                                  const DecomposedQueryId& decomposedQueryId,
                                  const ReconfigurationMarkerPtr& reconfigurationMarker,
                                  const CompletionQueuePtr& cq);

    /**
     * @brief Registers to a remote worker node its monitoring plan.
     * @param ipAddress
     * @param the monitoring plan
     * @return bool if successful
     */
    bool registerMonitoringPlan(const std::string& address, const Monitoring::MonitoringPlanPtr& plan);

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @param ipAddress
     * @return true if successful, else false
     */
    std::string requestMonitoringData(const std::string& address);

    /**
     * @brief Requests remote worker to start buffering data on a single NetworkSink identified by
     * a query sub plan Id and a global sinkId.
     * Once buffering starts, the Network Sink no longer sends data downstream
     * @param ipAddress
     * @param decomposedQueryId : the id of the query sub plan to which the Network Sink belongs
     * @param uniqueNetworkSinDescriptorId : unique id of the network sink descriptor. Used to find the Network Sink to buffer data on.
     * @return true if successful, else false
     */
    bool bufferData(const std::string& address, DecomposedQueryId decomposedQueryId, uint64_t uniqueNetworkSinDescriptorId);

    /**
     * @brief requests a remote worker to reconfigure a NetworkSink so that the NetworkSink changes where it sends data to (changes downstream node)
     * @param ipAddress
     * @param newNodeId : the id of the node that the Network Sink will send data to after reconfiguration
     * @param newHostname : the hostname of the node that the NetworkSink should send data to
     * @param newPort : the port of the node that the NetworkSink should send data to
     * @param decomposedQueryId : the id of the query sub plan to which the Network Sink belongs
     * @param uniqueNetworkSinDescriptorId : unique id of the network sink descriptor. Used to find the Network Sink to buffer data on.
     * @return true if successful, else false
     */
    bool updateNetworkSink(const std::string& address,
                           uint64_t newNodeId,
                           const std::string& newHostname,
                           uint32_t newPort,
                           DecomposedQueryId decomposedQueryId,
                           uint64_t uniqueNetworkSinDescriptorId);

    /**
     * @brief This functions loops over all queues and wait for the async calls return
     * @param rpcAsyncRequests: rpc requests made
     * @return true if all calls returned
     * @throws RpcException: Creates RPC exception with failedRPCCalls and mode
     */
    void checkAsyncResult(const std::vector<RpcAsyncRequest>& rpcAsyncRequests);

    /**
     * @brief method to propagate new epoch timestamp to source
     * @param timestamp: max timestamp of current epoch
     * @param queryId: query id which sources belong to
     * @param address: ip address of the source
     * @return bool indicating success
     */
    bool injectEpochBarrier(uint64_t timestamp, uint64_t queryId, const std::string& address);

    /**
     * @brief method to check the health of the worker
     * @param address: ip address of the source
     * @return bool indicating success
     */
    bool checkHealth(const std::string& address, std::string healthServiceName);

    /**
     * @brief method to check the location of any node. If the node is a mobile node, its current location will be returned.
     * If the node is a field node, its fixed location will be returned. If the node does not have a known location, an
     * invalid location will be returned
     * @param address: the ip address of the node
     * @return location representing the nodes location or invalid if no such location exists
     */
    NES::Spatial::DataTypes::Experimental::Waypoint getWaypoint(const std::string& address);

    void startBufferingAsync(std::string address, const CompletionQueuePtr& cq, WorkerId newParent);
    void sendCheckPointToSource(std::string address, const CompletionQueuePtr& cq, const CheckPointList& checkPoint);

  private:
    WorkerRPCClient() = default;
};
}// namespace NES

#endif// NES_COORDINATOR_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_
