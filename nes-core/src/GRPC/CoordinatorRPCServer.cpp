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

#include <GRPC/CoordinatorRPCServer.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/MonitoringManager.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/SourceCatalogService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Util/Logger/Logger.hpp>

using namespace NES;

CoordinatorRPCServer::CoordinatorRPCServer(TopologyManagerServicePtr topologyManagerService,
                                           SourceCatalogServicePtr sourceCatalogService,
                                           QueryCatalogServicePtr queryCatalogService,
                                           MonitoringManagerPtr monitoringManager,
                                           ReplicationServicePtr replicationService)
    : topologyManagerService(topologyManagerService), sourceCatalogService(sourceCatalogService),
      queryCatalogService(queryCatalogService), monitoringManager(monitoringManager), replicationService(replicationService){};

Status CoordinatorRPCServer::RegisterNode(ServerContext*, const RegisterNodeRequest* request, RegisterNodeReply* reply) {
    uint64_t id;
    if (request->has_coordinates()) {
        NES_DEBUG("TopologyManagerService::RegisterNode: request =" << request);
        id = topologyManagerService->registerNode(request->address(),
                                                  request->grpcport(),
                                                  request->dataport(),
                                                  request->numberofslots(),
                                                  request->ismobile(),
                                                  NES::Spatial::Index::Experimental::Location(request->coordinates()));
    } else {
        /* if we did not get a valid location via the request, just pass an invalid location by using the default constructor
        of geographical location */
        id = topologyManagerService->registerNode(request->address(),
                                                  request->grpcport(),
                                                  request->dataport(),
                                                  request->numberofslots(),
                                                  request->ismobile(),
                                                  NES::Spatial::Index::Experimental::Location());
    }

    auto registrationMetrics =
        std::make_shared<Metric>(RegistrationMetrics(request->registrationmetrics()), MetricType::RegistrationMetric);
    registrationMetrics->getValue<RegistrationMetrics>().nodeId = id;
    monitoringManager->addMonitoringData(id, registrationMetrics);

    if (id != 0) {
        NES_DEBUG("CoordinatorRPCServer::RegisterNode: success id=" << id);
        reply->set_id(id);
        return Status::OK;
    }
    NES_DEBUG("CoordinatorRPCServer::RegisterNode: failed");
    reply->set_id(0);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::UnregisterNode(ServerContext*, const UnregisterNodeRequest* request, UnregisterNodeReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterNode: request =" << request);

    bool success = topologyManagerService->unregisterNode(request->id());
    if (success) {
        monitoringManager->removeMonitoringNode(request->id());
        NES_DEBUG("CoordinatorRPCServer::UnregisterNode: sensor successfully removed");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::UnregisterNode: sensor was not removed");
    reply->set_success(false);
    return Status::CANCELLED;
}


// TODO: weiter machen
//Status CoordinatorRPCServer::RegisterMonitoringPlan(ServerContext*, const RegisterMonitoringPlanRequest* request, RegisterMonitoringPlanReply* reply) {
//    NES_DEBUG("CoordinatorRPCServer::RegisterMonitoringPlan: request =" << request);
//
//    bool success = monitoringManager->unregisterNode(request->id());
//    if (success) {
//        monitoringManager->removeMonitoringNode(request->id());
//        NES_DEBUG("CoordinatorRPCServer::UnregisterNode: sensor successfully removed");
//        reply->set_success(true);
//        return Status::OK;
//    }
//    NES_ERROR("CoordinatorRPCServer::UnregisterNode: sensor was not removed");
//    reply->set_success(false);
//    return Status::CANCELLED;
//}

Status CoordinatorRPCServer::LogicalSourceLookUp(ServerContext*, const LogicalSourceLookUpRequest* request, LogicalSourceLookUpReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::LogicalSourceLookUp: request =" << request);

    bool success = sourceCatalogService->logicalSourceLookUp(request->logicalsourcename());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::LogicalSourceLookUp: LogicalSource does already exist");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::LogicalSourceLookUp: LogicalSource does not already exist");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::RegisterPhysicalSource(ServerContext*,
                                                    const RegisterPhysicalSourcesRequest* request,
                                                    RegisterPhysicalSourcesReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterPhysicalSource: request =" << request);
    TopologyNodePtr physicalNode = this->topologyManagerService->findNodeWithId(request->id());
    for (const auto& physicalSourceDefinition : request->physicalsources()) {
        bool success = sourceCatalogService->registerPhysicalSource(physicalNode,
                                                                    physicalSourceDefinition.physicalsourcename(),
                                                                    physicalSourceDefinition.logicalsourcename());
        if (!success) {
            NES_ERROR("CoordinatorRPCServer::RegisterPhysicalSource failed");
            reply->set_success(false);
            return Status::CANCELLED;
        }
    }
    NES_DEBUG("CoordinatorRPCServer::RegisterPhysicalSource Succeed");
    reply->set_success(true);
    return Status::OK;
}

Status CoordinatorRPCServer::UnregisterPhysicalSource(ServerContext*,
                                                      const UnregisterPhysicalSourceRequest* request,
                                                      UnregisterPhysicalSourceReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterPhysicalSource: request =" << request);

    TopologyNodePtr physicalNode = this->topologyManagerService->findNodeWithId(request->id());
    bool success =
        sourceCatalogService->unregisterPhysicalSource(physicalNode, request->physicalsourcename(), request->logicalsourcename());

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
    NES_DEBUG("CoordinatorRPCServer::RegisterLogicalSource: request =" << request);

    bool success = sourceCatalogService->registerLogicalSource(request->logicalsourcename(), request->sourceschema());

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
    NES_DEBUG("CoordinatorRPCServer::UnregisterLogicalSource: request =" << request);

    bool success = sourceCatalogService->unregisterLogicalSource(request->logicalsourcename());
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
    NES_DEBUG("CoordinatorRPCServer::AddParent: request =" << request);

    bool success = topologyManagerService->addParent(request->childid(), request->parentid());
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
    NES_DEBUG("CoordinatorRPCServer::ReplaceParent: request =" << request);

    bool success = topologyManagerService->removeParent(request->childid(), request->oldparent());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::ReplaceParent success removeParent");
        bool success2 = topologyManagerService->addParent(request->childid(), request->newparent());
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
    NES_DEBUG("CoordinatorRPCServer::RemoveParent: request =" << request);

    bool success = topologyManagerService->removeParent(request->childid(), request->parentid());
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
        NES_ERROR("CoordinatorRPCServer::notifyQueryFailure: failure message received. id of failed query: "
                  << request->queryid() << "Id of worker: " << request->workerid()
                  << " Reason for failure: " << request->errormsg());
        // TODO implement here what happens with received Query that failed
        reply->set_success(true);
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("CoordinatorRPCServer: received broken failure message: " << ex.what());
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::NotifyEpochTermination(ServerContext*,
                                                    const EpochBarrierPropagationNotification* request,
                                                    EpochBarrierPropagationReply* reply) {
    try {
        NES_INFO("CoordinatorRPCServer::propagatePunctuation: received punctuation with timestamp "
                 << request->timestamp() << "and querySubPlanId " << request->queryid());
        this->replicationService->notifyEpochTermination(request->timestamp(), request->queryid());
        reply->set_success(true);
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("CoordinatorRPCServer: received broken punctuation message: " << ex.what());
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::GetNodesInRange(ServerContext*, const GetNodesInRangeRequest* request, GetNodesInRangeReply* reply) {

    std::vector<std::pair<uint64_t, NES::Spatial::Index::Experimental::Location>> inRange =
        topologyManagerService->getNodesIdsInRange(NES::Spatial::Index::Experimental::Location(request->coord()),
                                                   request->radius());

    for (auto elem : inRange) {
        WorkerLocationInfo* workerInfo = reply->add_nodes();
        workerInfo->set_id(elem.first);
        workerInfo->set_allocated_coord(new Coordinates{elem.second});
    }
    return Status::OK;
}

Status CoordinatorRPCServer::SendErrors(ServerContext*, const SendErrorsMessage* request, ErrorReply* reply) {
    try {
        NES_ERROR("CoordinatorRPCServer::sendErrors: failure message received."
                  << "Id of worker: " << request->workerid() << " Reason for failure: " << request->errormsg());
        // TODO implement here what happens with received Error Messages
        reply->set_success(true);
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("CoordinatorRPCServer: received broken failure message: " << ex.what());
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::RequestSoftStop(::grpc::ServerContext*,
                                             const ::RequestSoftStopMessage* request,
                                             ::StopRequestReply* response) {

    //Check with query catalog service if the request possible
    auto queryId = request->queryid();
    auto subQueryPlanId = request->subqueryid();
    auto sourceId = request->sourceid();
    auto softStopPossible = queryCatalogService->checkAndMarkForSoftStop(queryId, subQueryPlanId, sourceId);

    //Send response
    response->set_success(softStopPossible);
    return Status::OK;
}

Status CoordinatorRPCServer::notifySourceStopTriggered(::grpc::ServerContext*,
                                                       const ::SoftStopTriggeredMessage* request,
                                                       ::SoftStopTriggeredReply* response) {
    //Fetch the request
    auto queryId = request->queryid();
    auto querySubPlanId = request->querysubplanid();
    auto sourceId = request->sourceid();

    //inform catalog service
    bool success = queryCatalogService->updateQuerySubPlanStatus(queryId, querySubPlanId, QueryStatus::SoftStopTriggered);

    //update response
    response->set_success(success);
    return Status::OK;
}

Status CoordinatorRPCServer::NotifySoftStopCompleted(::grpc::ServerContext*,
                                                     const ::SoftStopCompletionMessage* request,
                                                     ::SoftStopCompletionReply* response) {
    //Fetch the request
    auto queryId = request->queryid();
    auto querySubPlanId = request->querysubplanid();

    //inform catalog service
    bool success = queryCatalogService->updateQuerySubPlanStatus(queryId, querySubPlanId, QueryStatus::SoftStopCompleted);

    //update response
    response->set_success(success);
    return Status::OK;
}
