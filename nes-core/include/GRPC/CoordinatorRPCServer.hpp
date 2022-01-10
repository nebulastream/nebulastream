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

#ifndef NES_INCLUDE_GRPC_COORDINATORRPCSERVER_HPP_
#define NES_INCLUDE_GRPC_COORDINATORRPCSERVER_HPP_

#include <CoordinatorRPCService.grpc.pb.h>
#include <Services/StreamCatalogService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Services/ReplicationService.hpp>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace NES {

class TopologyManagerService;
using TopologyManagerServicePtr = std::shared_ptr<TopologyManagerService>;
class StreamCatalogService;
using StreamCatalogServicePtr = std::shared_ptr<StreamCatalogService>;
class MonitoringManager;
using MonitoringManagerPtr = std::shared_ptr<MonitoringManager>;

/**
 * @brief Coordinator RPC server responsible for receiving requests over GRPC interface
 */
class CoordinatorRPCServer final : public CoordinatorRPCService::Service {
  public:
    /**
     * @brief Create coordinator RPC server
     * @param topologyManagerService : the instance of the topologyManagerService
     * @param streamCatalogService : the instance of the steam catalog service
     * @param monitoringService: the instance of monitoring service
     */
    explicit CoordinatorRPCServer(TopologyManagerServicePtr topologyManagerService, StreamCatalogServicePtr streamCatalogService, MonitoringManagerPtr monitoringService, ReplicationServicePtr replicationService);

    /**
     * @brief RPC Call to register a node
     * @param context: the server context
     * @param request: node registration request
     * @param reply: the node registration reply
     * @return success
     */
    Status RegisterNode(ServerContext* context, const RegisterNodeRequest* request, RegisterNodeReply* reply) override;

    /**
     * @brief RPC Call to unregister a node
     * @param context: the server context
     * @param request: node unregistration request
     * @param reply: the node unregistration reply
     * @return success
     */
    Status UnregisterNode(ServerContext* context, const UnregisterNodeRequest* request, UnregisterNodeReply* reply) override;

    /**
     * @brief RPC Call to register physical stream
     * @param context: the server context
     * @param request: register physical source request
     * @param reply: register physical source response
     * @return success
     */
    Status RegisterPhysicalSource(ServerContext* context,
                                  const RegisterPhysicalSourcesRequest* request,
                                  RegisterPhysicalSourcesReply* reply) override;

    /**
     * @brief RPC Call to unregister physical stream
     * @param context: the server context
     * @param request: unregister physical source request
     * @param reply: unregister physical source reply
     * @return success
     */
    Status UnregisterPhysicalSource(ServerContext* context,
                                    const UnregisterPhysicalSourceRequest* request,
                                    UnregisterPhysicalSourceReply* reply) override;

    /**
     * @brief RPC Call to register logical stream
     * @param context: the server context
     * @param request: register logical source request
     * @param reply: register logical source response
     * @return success
     */
    Status RegisterLogicalSource(ServerContext* context,
                                 const RegisterLogicalSourceRequest* request,
                                 RegisterLogicalSourceReply* reply) override;

    /**
     * @brief RPC Call to unregister logical stream
     * @param context: the server context
     * @param request: unregister logical source request
     * @param reply: unregister logical source response
     * @return success
     */
    Status UnregisterLogicalSource(ServerContext* context,
                                   const UnregisterLogicalSourceRequest* request,
                                   UnregisterLogicalSourceReply* reply) override;

    /**
     * @brief RPC Call to add parent
     * @param context: the server context
     * @param request: add parent request
     * @param reply: add parent reply
     * @return success
     */
    Status AddParent(ServerContext* context, const AddParentRequest* request, AddParentReply* reply) override;

    /**
     * @brief RPC Call to replace parent
     * @param context: the server context
     * @param request: replace parent request
     * @param reply: replace parent reply
     * @return success
     */
    Status ReplaceParent(ServerContext* context, const ReplaceParentRequest* request, ReplaceParentReply* reply) override;

    /**
     * @brief RPC Call to remove parent
     * @param context: the server context
     * @param request: remove parent request
     * @param reply: remove parent response
     * @return success
     */
    Status RemoveParent(ServerContext* context, const RemoveParentRequest* request, RemoveParentReply* reply) override;

    /**
     * @brief RPC Call to notify the failure of a query
     * @param context: the server context
     * @param request that is sent from worker to the coordinator and filled with information of the failed query (Ids of query, worker, etc. and error message)
     * @param reply that is sent back from the coordinator to the worker to confirm that notification was successful
     * @return success
     */
    Status NotifyQueryFailure(ServerContext* context,
                              const QueryFailureNotification* request,
                              QueryFailureNotificationReply* reply) override;

    /**
      * @brief RPC Call to propagate timestamp
      * @param context
      * @param request that is sent from worker to the coordinator, contains timestamp and queryId
      * @param reply that is sent back from the coordinator to the worker to confirm that notification was successful
      * @return success
      */
    Status NotifyEpochTermination(ServerContext* context,
                                const EpochBarrierPropagationNotification* request,
                                EpochBarrierPropagationReply* reply) override;


    /**
     * @brief RPC Call to get a list of field nodes within a defined radius around a geographical location
     * @param context: the server context
     * @param request that is sent from worker to the coordinator containing the center of the query area and the radius
     * @param reply that is sent back from the coordinator to the worker containing the ids of all nodes in the defined area and their corresponding locations
     * @return success
     */
    Status GetNodesInRange(ServerContext*, const GetNodesInRangeRequest* request, GetNodesInRangeReply* reply) override;

  private:
    TopologyManagerServicePtr topologyManagerService;
    StreamCatalogServicePtr streamCatalogService;
    MonitoringManagerPtr monitoringManager;
    ReplicationServicePtr replicationService;
};
}// namespace NES

#endif  // NES_INCLUDE_GRPC_COORDINATORRPCSERVER_HPP_
