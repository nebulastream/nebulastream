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
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddLinkEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPOffloadQueryEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveLinkEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPRequest.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddPhysicalSourcesEvent.hpp>
#include <Runtime/OpenCLManager.hpp>
#include <Services/CoordinatorHealthCheckService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/ReconnectPoint.hpp>
#include <Util/Mobility/SpatialTypeUtility.hpp>
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
    auto configWorkerId = WorkerId(registrationRequest->workerid());
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
        reply->set_workerid(workerId.getRawValue());
        return Status::OK;
    }
    NES_DEBUG("CoordinatorRPCServer::RegisterNode: failed");
    reply->set_workerid(INVALID_WORKER_NODE_ID.getRawValue());
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::RequestQueryOffload(ServerContext*,
                                                 const RequestQueryOffloadRequest* request,
                                                 RequestQueryOffloadResponse* reply) {
    NES_DEBUG("CoordinatorRPCServer::RequestQueryOffload: Received request for shared_query_id={}", request->sharedqueryid());

    WorkerId originWorkerId = WorkerId(request->originworkerid());
    SharedQueryId sharedQueryId(request->sharedqueryid());
    DecomposedQueryId decomposedQueryId(request->decomposedqueryid());
    WorkerId targetWorkerId = WorkerId(request->targetworkerid());


    bool offloadSuccess = false;
    std::string offloadMessage;

    try {
        auto event = NES::RequestProcessor::ISQPOffloadQueryEvent::create(originWorkerId, sharedQueryId, decomposedQueryId, targetWorkerId);
        auto responseFuture = requestHandlerService->queueISQPRequest({event});

        offloadMessage = "Offload request accepted and scheduled.";
        offloadSuccess = true;
        NES_INFO("CoordinatorRPCServer::RequestQueryOffload: offload successful for query {}", sharedQueryId);
    } catch (const std::exception& ex) {
        offloadSuccess = false;
        offloadMessage = std::string("Exception while attempting offload: ") + ex.what();
        NES_ERROR("CoordinatorRPCServer::RequestQueryOffload: exception: {}", ex.what());
    }

    reply->set_success(offloadSuccess);
    reply->set_message(offloadMessage);

    return Status::OK;
}

Status
CoordinatorRPCServer::UnregisterWorker(ServerContext*, const UnregisterWorkerRequest* request, UnregisterWorkerReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterNode: request ={}", request->DebugString());

    auto workerId = WorkerId(request->workerid());
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
    bool overallSuccess = true;
    for (const auto& physicalSourceDefinition : request->physicalsourcetypes()) {
        std::vector<RequestProcessor::PhysicalSourceDefinition> currentPhysicalSourceDefinition;
        currentPhysicalSourceDefinition.emplace_back(physicalSourceDefinition.logicalsourcename(),
                                                     physicalSourceDefinition.physicalsourcename(),
                                                     physicalSourceDefinition.sourcetype());
        auto success = requestHandlerService->queueRegisterPhysicalSourceRequest(currentPhysicalSourceDefinition,
                                                                                 WorkerId(request->workerid()));
        auto result = reply->add_results();
        result->set_physicalsourcename(physicalSourceDefinition.physicalsourcename());
        result->set_success(success);
        overallSuccess &= success;
        if (!success) {
            NES_ERROR("CoordinatorRPCServer::RegisterPhysicalSource failed");
            result->set_reason("failed to RegisterPhysicalSource");
        }
    }

    // Rollback any source registrations if any source failed. Currently we assume that the worker is going to fail,
    // and reattempts to register all sources
    if (!overallSuccess) {
        NES_WARNING("CoordinatorRPCServer::RegisterPhysicalSource Could not register all physical sources");
        requestHandlerService->queueUnregisterAllPhysicalSourcesByWorkerRequest(WorkerId(request->workerid()));
    }
    reply->set_success(overallSuccess);
    NES_DEBUG("CoordinatorRPCServer::RegisterPhysicalSource: request done success={}", overallSuccess);
    return Status::OK;
}

Status CoordinatorRPCServer::UnregisterPhysicalSource(ServerContext*,
                                                      const UnregisterPhysicalSourceRequest* request,
                                                      UnregisterPhysicalSourceReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterPhysicalSource: request ={}", request->DebugString());

    bool success = requestHandlerService->queueUnregisterPhysicalSourceRequest(request->physicalsourcename(),
                                                                               request->logicalsourcename(),
                                                                               WorkerId(request->workerid()));

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

    bool success = topology->addTopologyNodeAsChild(WorkerId(request->parentid()), WorkerId(request->childid()));
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

    bool success = topology->removeTopologyNodeAsChild(WorkerId(request->oldparent()), WorkerId(request->childid()));
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::ReplaceParent success removeAsParent");
        bool success2 = topology->addTopologyNodeAsChild(WorkerId(request->newparent()), WorkerId(request->childid()));
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

    bool success = topology->removeTopologyNodeAsChild(WorkerId(request->parentid()), WorkerId(request->childid()));
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
                  request->decomposedqueryid(),
                  request->workerid(),
                  request->errormsg());

        NES_ASSERT2_FMT(!request->errormsg().empty(),
                        "Cannot fail query without error message " << request->queryid()
                                                                   << " subplan: " << request->decomposedqueryid()
                                                                   << " from worker: " << request->workerid());

        auto sharedQueryId = SharedQueryId(request->queryid());
        auto decomposedQueryId = DecomposedQueryId(request->decomposedqueryid());

        //Send one failure request for the shared query plan
        if (!requestHandlerService->validateAndQueueFailQueryRequest(sharedQueryId,
                                                                     decomposedQueryId,
                                                                     INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                                     request->errormsg())) {
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

    auto inRange = topology->getTopologyNodeIdsInRange(NES::Spatial::DataTypes::Experimental::GeoLocation(request->geolocation()),
                                                       request->radius());

    for (auto elem : inRange) {
        NES::Spatial::Protobuf::WorkerLocationInfo* workerInfo = reply->add_nodes();
        auto geoLocation = elem.second;
        workerInfo->set_id(elem.first.getRawValue());
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
    auto sharedQueryId = SharedQueryId(request->queryid());
    auto decomposedQueryId = DecomposedQueryId(request->decomposedqueryid());

    NES_WARNING("CoordinatorRPCServer: received request for soft stopping the shared query plan id: {}", sharedQueryId)

    //Check with query catalog service if the request possible
    auto softStopPossible = queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                          decomposedQueryId,
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
    auto sharedQueryId = SharedQueryId(request->queryid());
    auto decomposedQueryId = DecomposedQueryId(request->decomposedqueryid());
    NES_INFO("CoordinatorRPCServer: received request for soft stopping the sub pan : {}  shared query plan id:{}",
             decomposedQueryId,
             sharedQueryId)

    //inform catalog service
    bool success = queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                 decomposedQueryId,
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
    auto sharedQueryId = SharedQueryId(request->queryid());
    auto decomposedQueryId = DecomposedQueryId(request->decomposedqueryid());

    //inform catalog service
    bool success = queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                 decomposedQueryId,
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
        addedReconnects.emplace_back(
            NES::Spatial::Mobility::Experimental::ReconnectPoint{location, WorkerId(toAdd.id()), toAdd.time()});
    }
    auto reconnectsToRemoveMessage = request->removereconnects();
    std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint> removedReconnects;

    for (const auto& toRemove : reconnectsToRemoveMessage) {
        NES::Spatial::DataTypes::Experimental::GeoLocation location(toRemove.geolocation());
        removedReconnects.emplace_back(
            NES::Spatial::Mobility::Experimental::ReconnectPoint{location, WorkerId(toRemove.id()), toRemove.time()});
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
    if (!topology->updateGeoLocation(WorkerId(request->workerid()), std::move(geoLocation))) {
        reply->set_success(true);
        return Status::OK;
    }
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::GetParents(ServerContext*, const GetParentsRequest* request, GetParentsReply* reply) {
    auto nodeId = WorkerId(request->nodeid());
    auto parentIds = topology->getParentTopologyNodeIds(nodeId);
    if (!parentIds.empty()) {
        auto replyParents = reply->mutable_parentids();
        replyParents->Reserve(parentIds.size());
        for (const auto& parentId : parentIds) {
            auto newId = replyParents->Add();
            *newId = parentId.getRawValue();
        }
        return Status::OK;
    }
    return Status::CANCELLED;
}

Status
CoordinatorRPCServer::RelocateTopologyNode(ServerContext*, const NodeRelocationRequest* request, NodeRelocationReply* reply) {
    std::vector<RequestProcessor::ISQPEventPtr> isqpEvents;
    for (const auto& removedTopologyLink : request->removedlinks()) {
        isqpEvents.emplace_back(RequestProcessor::ISQPRemoveLinkEvent::create(WorkerId(removedTopologyLink.upstream()),
                                                                              WorkerId(removedTopologyLink.downstream())));
    }
    for (const auto& addedTopologyLink : request->addedlinks()) {
        isqpEvents.emplace_back(RequestProcessor::ISQPAddLinkEvent::create(WorkerId(addedTopologyLink.upstream()),
                                                                           WorkerId(addedTopologyLink.downstream())));
    }

    auto isqpRequestResponse = requestHandlerService->queueISQPRequest(isqpEvents);
    reply->set_success(isqpRequestResponse->success);
    if (isqpRequestResponse) {
        return Status::OK;
    }
    return Status::CANCELLED;
}
