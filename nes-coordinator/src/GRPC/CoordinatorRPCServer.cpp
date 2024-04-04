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

#include <Catalogs/Exceptions/InvalidQueryStateException.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Components/NesCoordinator.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <GRPC/CoordinatorRPCServer.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/MonitoringManager.hpp>
#include <Runtime/OpenCLManager.hpp>
#include <Services/CoordinatorHealthCheckService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/ReconnectPoint.hpp>
#include <Util/Mobility/SpatialTypeUtility.hpp>
#include <Util/TopologyLinkInformation.hpp>
#include <RequestProcessor/RequestTypes/UpdateSourceCatalogRequest.hpp>
#include <utility>

using namespace NES;

// Helper method to deserialize information about the OpenCL devices of a worker.
void deserializeOpenCLDeviceInfo(std::any& property,
                                 const google::protobuf::RepeatedPtrField<SerializedOpenCLDeviceInfo>& serializedDeviceInfos) {
    std::vector<NES::Runtime::OpenCLDeviceInfo> devices;
    for (const auto& serializedDeviceInfo : serializedDeviceInfos) {
        if (serializedDeviceInfo.maxworkitems_size() != 3) {
            NES_WARNING("OpenCL device {} {} {} has invalid number of maxWorkItems: {}; skipping.",
                        serializedDeviceInfo.platformvendor(),
                        serializedDeviceInfo.platformname(),
                        serializedDeviceInfo.devicename(),
                        serializedDeviceInfo.maxworkitems_size());
            continue;
        }
        std::array<size_t, 3> maxWorkItems{serializedDeviceInfo.maxworkitems(0),
                                           serializedDeviceInfo.maxworkitems(1),
                                           serializedDeviceInfo.maxworkitems(2)};
        devices.emplace_back(serializedDeviceInfo.platformvendor(),
                             serializedDeviceInfo.platformname(),
                             serializedDeviceInfo.devicename(),
                             serializedDeviceInfo.doublefpsupport(),
                             maxWorkItems,
                             serializedDeviceInfo.deviceaddressbits(),
                             serializedDeviceInfo.devicetype(),
                             serializedDeviceInfo.deviceextensions(),
                             serializedDeviceInfo.availableprocessors(),
                             serializedDeviceInfo.globalmemory());
    }
    property = devices;
}

CoordinatorRPCServer::CoordinatorRPCServer(RequestHandlerServicePtr requestHandlerService,
                                           TopologyPtr topology,
                                           Catalogs::Query::QueryCatalogPtr queryCatalog,
                                           Monitoring::MonitoringManagerPtr monitoringManager,
                                           QueryParsingServicePtr queryParsingService,
                                           CoordinatorHealthCheckServicePtr coordinatorHealthCheckService)
    : requestHandlerService(std::move(requestHandlerService)), topology(std::move(topology)),
      queryCatalog(std::move(queryCatalog)), monitoringManager(std::move(monitoringManager)),
      queryParsingService(std::move(queryParsingService)),
      coordinatorHealthCheckService(std::move(coordinatorHealthCheckService)){};

Status CoordinatorRPCServer::RegisterWorker(ServerContext*,
                                            const RegisterWorkerRequest* registrationRequest,
                                            RegisterWorkerReply* reply) {

    NES_DEBUG("Received worker registration request {}", registrationRequest->DebugString());
    auto configWorkerId = registrationRequest->workerid();
    auto address = registrationRequest->address();
    auto grpcPort = registrationRequest->grpcport();
    auto dataPort = registrationRequest->dataport();
    auto slots = registrationRequest->numberofslots();
    auto bandwidthInMbps = registrationRequest->bandwidthinmbps();
    auto latencyInMs = registrationRequest->latencyinms();
    //construct worker property from the request
    std::map<std::string, std::any> workerProperties;
    workerProperties[NES::Worker::Properties::MAINTENANCE] = false;// During registration,
                                                                   // we assume that the node is not under maintenance
    workerProperties[NES::Worker::Configuration::TENSORFLOW_SUPPORT] = registrationRequest->tfsupported();
    workerProperties[NES::Worker::Configuration::JAVA_UDF_SUPPORT] = registrationRequest->javaudfsupported();
    workerProperties[NES::Worker::Configuration::SPATIAL_SUPPORT] =
        NES::Spatial::Util::SpatialTypeUtility::protobufEnumToNodeType(registrationRequest->spatialtype());
    deserializeOpenCLDeviceInfo(workerProperties[NES::Worker::Configuration::OPENCL_DEVICES],
                                registrationRequest->opencldevices());

    // check if an inactive worker with configWorkerId already exists
    // else assign an invalid worker id
    if (coordinatorHealthCheckService && coordinatorHealthCheckService->isWorkerInactive(configWorkerId)) {
        // node is re-registering (was inactive and became active again)
        NES_TRACE("TopologyManagerService::registerWorker: node with worker id {} is re-registering", configWorkerId);
        coordinatorHealthCheckService->removeNodeFromHealthCheck(configWorkerId);
        // Remove the old topology node
        topology->unregisterWorker(configWorkerId);
    } else {
        configWorkerId = INVALID_WORKER_NODE_ID;
    }

    NES_DEBUG("TopologyManagerService::RegisterNode: request ={}", registrationRequest->DebugString());
    WorkerId workerId =
        topology
            ->registerWorker(configWorkerId, address, grpcPort, dataPort, slots, workerProperties, bandwidthInMbps, latencyInMs);

    NES::Spatial::DataTypes::Experimental::GeoLocation geoLocation(registrationRequest->waypoint().geolocation().lat(),
                                                                   registrationRequest->waypoint().geolocation().lng());

    if (!topology->addGeoLocation(workerId, std::move(geoLocation))) {
        NES_ERROR("Unable to update geo location of the topology");
        reply->set_workerid(0);
        return Status::CANCELLED;
    }

    auto registrationMetrics =
        std::make_shared<Monitoring::Metric>(Monitoring::RegistrationMetrics(registrationRequest->registrationmetrics()),
                                             Monitoring::MetricType::RegistrationMetric);
    registrationMetrics->getValue<Monitoring::RegistrationMetrics>().nodeId = workerId;
    monitoringManager->addMonitoringData(workerId, registrationMetrics);

    if (coordinatorHealthCheckService) {
        //add node to health check
        std::string grpcAddress = address + ":" + std::to_string(grpcPort);
        coordinatorHealthCheckService->addNodeToHealthCheck(workerId, grpcAddress);
    }

    if (workerId != INVALID_WORKER_NODE_ID) {
        NES_DEBUG("CoordinatorRPCServer::RegisterNode: success id={}", workerId);
        reply->set_workerid(workerId);
        return Status::OK;
    }
    NES_DEBUG("CoordinatorRPCServer::RegisterNode: failed");
    reply->set_workerid(INVALID_WORKER_NODE_ID);
    return Status::CANCELLED;
}

Status
CoordinatorRPCServer::UnregisterWorker(ServerContext*, const UnregisterWorkerRequest* request, UnregisterWorkerReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterNode: request ={}", request->DebugString());

    google::protobuf::uint64 workerId = request->workerid();
    if (!topology->unregisterWorker(workerId)) {
        NES_ERROR("CoordinatorRPCServer::UnregisterNode: Worker was not removed");
        reply->set_success(false);
        return Status::CANCELLED;
    }

    if (coordinatorHealthCheckService) {
        //remove node to health check
        coordinatorHealthCheckService->removeNodeFromHealthCheck(workerId);
    }

    monitoringManager->removeMonitoringNode(workerId);
    NES_DEBUG("CoordinatorRPCServer::UnregisterNode: Worker successfully removed");
    reply->set_success(true);
    return Status::OK;
}

Status CoordinatorRPCServer::RegisterPhysicalSource(ServerContext*,
                                                    const RegisterPhysicalSourcesRequest* request,
                                                    RegisterPhysicalSourcesReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterPhysicalSource: request ={}", request->DebugString());
    std::vector<RequestProcessor::PhysicalSourceAddition> additions;
    for (const auto& physicalSourceDefinition : request->physicalsourcetypes()) {
        additions.emplace_back(physicalSourceDefinition.physicalsourcename(), physicalSourceDefinition.logicalsourcename(), request->workerid());
    }
        bool success = requestHandlerService->queueRegisterPhysicalSourceRequest(additions);
        if (!success) {
            NES_ERROR("CoordinatorRPCServer::RegisterPhysicalSource failed");
            reply->set_success(false);
            return Status::CANCELLED;
        }
    NES_DEBUG("CoordinatorRPCServer::RegisterPhysicalSource Succeed");
    reply->set_success(true);
    return Status::OK;
}

Status CoordinatorRPCServer::UnregisterPhysicalSource(ServerContext*,
                                                      const UnregisterPhysicalSourceRequest* request,
                                                      UnregisterPhysicalSourceReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterPhysicalSource: request ={}", request->DebugString());

    bool success = requestHandlerService->queueUnregisterPhysicalSourceRequest(request->physicalsourcename(),
                                                                               request->logicalsourcename(),
                                                                               request->workerid());

    if (success) {
        NES_DEBUG("CoordinatorRPCServer::UnregisterPhysicalSource success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::UnregisterPhysicalSource failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::RegisterLogicalSource(ServerContext*,
                                                   const RegisterLogicalSourceRequest* request,
                                                   RegisterLogicalSourceReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterLogicalSource: request = {}", request->DebugString());

    auto schema = queryParsingService->createSchemaFromCode(request->sourceschema());
    bool success = requestHandlerService->queueRegisterLogicalSourceRequest(request->logicalsourcename(), schema);

    if (success) {
        NES_DEBUG("CoordinatorRPCServer::RegisterLogicalSource success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::RegisterLogicalSource failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::UnregisterLogicalSource(ServerContext*,
                                                     const UnregisterLogicalSourceRequest* request,
                                                     UnregisterLogicalSourceReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterLogicalSource: request ={}", request->DebugString());

    auto success = requestHandlerService->queueUnregisterLogicalSourceRequest(request->logicalsourcename());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::UnregisterLogicalSource success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::UnregisterLogicalSource failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::AddParent(ServerContext*, const AddParentRequest* request, AddParentReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::AddParent: request = {}", request->DebugString());

    bool success = topology->addTopologyNodeAsChild(request->parentid(), request->childid());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::AddParent success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::AddParent failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::ReplaceParent(ServerContext*, const ReplaceParentRequest* request, ReplaceParentReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::ReplaceParent: request = {}", request->DebugString());

    bool success = topology->removeTopologyNodeAsChild(request->oldparent(), request->childid());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::ReplaceParent success removeAsParent");
        bool success2 = topology->addTopologyNodeAsChild(request->newparent(), request->childid());
        if (success2) {
            NES_DEBUG("CoordinatorRPCServer::ReplaceParent success addParent topo=");
            reply->set_success(true);
            return Status::OK;
        }
        NES_ERROR("CoordinatorRPCServer::ReplaceParent failed in addParent");
        reply->set_success(false);
        return Status::CANCELLED;

    } else {
        NES_ERROR("CoordinatorRPCServer::ReplaceParent failed in remove parent");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::RemoveParent(ServerContext*, const RemoveParentRequest* request, RemoveParentReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RemoveParent: request = {}", request->DebugString());

    bool success = topology->removeTopologyNodeAsChild(request->parentid(), request->childid());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::RemoveParent success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::RemoveParent failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::NotifyQueryFailure(ServerContext*,
                                                const QueryFailureNotification* request,
                                                QueryFailureNotificationReply* reply) {
    try {
        NES_ERROR("CoordinatorRPCServer::notifyQueryFailure: failure message received. id of failed query: {} subplan: {} Id of "
                  "worker: {} Reason for failure: {}",
                  request->queryid(),
                  request->subqueryid(),
                  request->workerid(),
                  request->errormsg());

        NES_ASSERT2_FMT(!request->errormsg().empty(),
                        "Cannot fail query without error message " << request->queryid() << " subplan: " << request->subqueryid()
                                                                   << " from worker: " << request->workerid());

        auto sharedQueryId = request->queryid();
        auto decomposedQueryPlanId = request->subqueryid();

        //Send one failure request for the shared query plan
        if (!requestHandlerService->validateAndQueueFailQueryRequest(sharedQueryId, decomposedQueryPlanId, request->errormsg())) {
            NES_ERROR("Failed to create Query Failure request for shared query plan {}", sharedQueryId);
            return Status::CANCELLED;
        }

        reply->set_success(true);
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("CoordinatorRPCServer: received broken failure message: {}", ex.what());
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::GetNodesInRange(ServerContext*, const GetNodesInRangeRequest* request, GetNodesInRangeReply* reply) {

    std::vector<std::pair<uint64_t, NES::Spatial::DataTypes::Experimental::GeoLocation>> inRange =
        topology->getTopologyNodeIdsInRange(NES::Spatial::DataTypes::Experimental::GeoLocation(request->geolocation()),
                                            request->radius());

    for (auto elem : inRange) {
        NES::Spatial::Protobuf::WorkerLocationInfo* workerInfo = reply->add_nodes();
        auto geoLocation = elem.second;
        workerInfo->set_id(elem.first);
        auto protoGeoLocation = workerInfo->mutable_geolocation();
        protoGeoLocation->set_lat(geoLocation.getLatitude());
        protoGeoLocation->set_lng(geoLocation.getLongitude());
    }
    return Status::OK;
}

Status CoordinatorRPCServer::SendErrors(ServerContext*, const SendErrorsMessage* request, ErrorReply* reply) {
    try {
        NES_ERROR("CoordinatorRPCServer::sendErrors: failure message received."
                  "Id of worker: {} Reason for failure: {}",
                  request->workerid(),
                  request->errormsg());
        // TODO implement here what happens with received Error Messages
        reply->set_success(true);
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("CoordinatorRPCServer: received broken failure message: {}", ex.what());
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::RequestSoftStop(::grpc::ServerContext*,
                                             const ::RequestSoftStopMessage* request,
                                             ::StopRequestReply* response) {
    auto sharedQueryId = request->queryid();
    auto decomposedQueryPlanId = request->subqueryid();
    NES_WARNING("CoordinatorRPCServer: received request for soft stopping the shared query plan id: {}", sharedQueryId)

    //Check with query catalog service if the request possible
    auto softStopPossible = queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                          decomposedQueryPlanId,
                                                                          INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                                          QueryState::MARKED_FOR_SOFT_STOP,
                                                                          INVALID_WORKER_NODE_ID);

    //Send response
    response->set_success(softStopPossible);
    return Status::OK;
}

Status CoordinatorRPCServer::notifySourceStopTriggered(::grpc::ServerContext*,
                                                       const ::SoftStopTriggeredMessage* request,
                                                       ::SoftStopTriggeredReply* response) {
    auto sharedQueryId = request->queryid();
    auto decomposedQueryPlanId = request->querysubplanid();
    NES_INFO("CoordinatorRPCServer: received request for soft stopping the sub pan : {}  shared query plan id:{}",
             decomposedQueryPlanId,
             sharedQueryId)

    //inform catalog service
    bool success = queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                 decomposedQueryPlanId,
                                                                 INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                                 QueryState::SOFT_STOP_TRIGGERED,
                                                                 INVALID_WORKER_NODE_ID);

    //update response
    response->set_success(success);
    return Status::OK;
}

Status CoordinatorRPCServer::NotifySoftStopCompleted(::grpc::ServerContext*,
                                                     const ::SoftStopCompletionMessage* request,
                                                     ::SoftStopCompletionReply* response) {
    //Fetch the request
    auto sharedQueryId = request->queryid();
    auto decomposedQueryPlanId = request->querysubplanid();

    //inform catalog service
    bool success = queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                 decomposedQueryPlanId,
                                                                 INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                                 QueryState::SOFT_STOP_COMPLETED,
                                                                 INVALID_WORKER_NODE_ID);

    //update response
    response->set_success(success);
    return Status::OK;
}

Status CoordinatorRPCServer::SendScheduledReconnect(ServerContext*,
                                                    const SendScheduledReconnectRequest* request,
                                                    SendScheduledReconnectReply* reply) {
    auto reconnectsToAddMessage = request->addreconnects();
    std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint> addedReconnects;
    for (const auto& toAdd : reconnectsToAddMessage) {
        NES::Spatial::DataTypes::Experimental::GeoLocation location(toAdd.geolocation());
        addedReconnects.emplace_back(NES::Spatial::Mobility::Experimental::ReconnectPoint{location, toAdd.id(), toAdd.time()});
    }
    auto reconnectsToRemoveMessage = request->removereconnects();
    std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint> removedReconnects;

    for (const auto& toRemove : reconnectsToRemoveMessage) {
        NES::Spatial::DataTypes::Experimental::GeoLocation location(toRemove.geolocation());
        removedReconnects.emplace_back(
            NES::Spatial::Mobility::Experimental::ReconnectPoint{location, toRemove.id(), toRemove.time()});
    }
    //FIXME: Call the code to update the predictions
    bool success = false;
    reply->set_success(success);
    return Status::CANCELLED;
}

Status
CoordinatorRPCServer::SendLocationUpdate(ServerContext*, const LocationUpdateRequest* request, LocationUpdateReply* reply) {
    auto coordinates = request->waypoint().geolocation();
    auto timestamp = request->waypoint().timestamp();
    NES_DEBUG("Coordinator received location update from node with id {} which reports [{}, {}] at TS {}",
              request->workerid(),
              coordinates.lat(),
              coordinates.lng(),
              timestamp);
    //todo #2862: update coordinator trajectory prediction
    auto geoLocation = NES::Spatial::DataTypes::Experimental::GeoLocation(coordinates);
    if (!topology->updateGeoLocation(request->workerid(), std::move(geoLocation))) {
        reply->set_success(true);
        return Status::OK;
    }
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::GetParents(ServerContext*, const GetParentsRequest* request, GetParentsReply* reply) {
    auto nodeId = request->nodeid();
    auto parentIds = topology->getParentTopologyNodeIds(nodeId);
    if (!parentIds.empty()) {
        auto replyParents = reply->mutable_parentids();
        replyParents->Reserve(parentIds.size());
        for (const auto& parentId : parentIds) {
            auto newId = replyParents->Add();
            *newId = parentId;
        }
        return Status::OK;
    }
    return Status::CANCELLED;
}

Status
CoordinatorRPCServer::RelocateTopologyNode(ServerContext*, const NodeRelocationRequest* request, NodeRelocationReply* reply) {
    std::vector<TopologyLinkInformation> removedLinks;
    removedLinks.reserve(request->removedlinks_size());
    for (const auto& removedTopologyLink : request->removedlinks()) {
        removedLinks.push_back(TopologyLinkInformation(removedTopologyLink.upstream(), removedTopologyLink.downstream()));
    }
    std::vector<TopologyLinkInformation> addedLinks;
    addedLinks.reserve((request->addedlinks_size()));
    for (const auto& addedTopologyLink : request->addedlinks()) {
        addedLinks.push_back(TopologyLinkInformation(addedTopologyLink.upstream(), addedTopologyLink.downstream()));
    }
    auto success = requestHandlerService->queueNodeRelocationRequest(removedLinks, addedLinks);
    reply->set_success(success);
    if (success) {
        return Status::OK;
    }
    return Status::CANCELLED;
}