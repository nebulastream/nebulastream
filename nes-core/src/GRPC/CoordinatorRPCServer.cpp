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
#include <Monitoring/MetricValues/GroupedMetricValues.hpp>
#include <Monitoring/MetricValues/StaticNesMetrics.hpp>
#include <Monitoring/MonitoringManager.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Util/Logger.hpp>

using namespace NES;

CoordinatorRPCServer::CoordinatorRPCServer(TopologyManagerServicePtr topologyManagerService,
                                           StreamCatalogServicePtr streamCatalogService,
                                           MonitoringManagerPtr monitoringManager,
                                           ReplicationServicePtr replicationService)
    : topologyManagerService(topologyManagerService), streamCatalogService(streamCatalogService),
      monitoringManager(monitoringManager), replicationService(replicationService){};

Status CoordinatorRPCServer::RegisterNode(ServerContext*, const RegisterNodeRequest* request, RegisterNodeReply* reply) {
    std::optional<std::tuple<double, double>> coordOpt;
    if (request->has_coordinates()) {
        const auto& coordIn = request->coordinates();
        coordOpt = std::make_tuple(coordIn.lat(), coordIn.lng());

    } else {
        coordOpt = std::optional<std::tuple<double, double>>();
    }

    NES_DEBUG("TopologyManagerService::RegisterNode: request =" << request);
    uint64_t id = topologyManagerService->registerNode(request->address(),
                                                       request->grpcport(),
                                                       request->dataport(),
                                                       request->numberofslots(),
                                                       coordOpt);

    auto groupedMetrics = std::make_shared<GroupedMetricValues>();
    groupedMetrics->staticNesMetrics = std::make_unique<StaticNesMetrics>(StaticNesMetrics(request->monitoringdata()));
    monitoringManager->receiveMonitoringData(id, groupedMetrics);

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

Status CoordinatorRPCServer::RegisterPhysicalSource(ServerContext*,
                                                    const RegisterPhysicalSourcesRequest* request,
                                                    RegisterPhysicalSourcesReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterPhysicalSource: request =" << request);
    TopologyNodePtr physicalNode = this->topologyManagerService->findNodeWithId(request->id());
    for (const auto& physicalSourceDefinition : request->physicalsources()) {
        bool success = streamCatalogService->registerPhysicalStream(physicalNode,
                                                                    physicalSourceDefinition.physicalsourcename(),
                                                                    physicalSourceDefinition.logicalsourcename());
        if (!success) {
            NES_ERROR("CoordinatorRPCServer::RegisterPhysicalSource success");
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
        streamCatalogService->unregisterPhysicalStream(physicalNode, request->physicalsourcename(), request->logicalsourcename());

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

    bool success = streamCatalogService->registerLogicalSource(request->logicalsourcename(), request->sourceschema());

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

    bool success = streamCatalogService->unregisterLogicalStream(request->logicalsourcename());
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

    auto middlePoint = std::make_tuple(request->coord().lat(), request->coord().lng());
    std::vector<std::pair<uint64_t , std::tuple<double, double>>> resultVec = topologyManagerService->getNodesIdsInRange(middlePoint, request->radius());

    for (auto elem : resultVec) {
        NodeGeoInfo* nodeInfo = reply->add_nodes();
        nodeInfo->set_id(elem.first);
        Coordinates coord;
        auto coordTuple = elem.second;
        coord.set_lat(std::get<0>(coordTuple));
        coord.set_lng(std::get<1>(coordTuple));
    }
    return Status::OK;
}
