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

#include <GRPC/CoordinatorRPCServer.hpp>
#include <Monitoring/MetricValues/GroupedMetricValues.hpp>
#include <Monitoring/MetricValues/StaticNesMetrics.hpp>
#include <Monitoring/MonitoringManager.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Util/Logger.hpp>
#include <utility>
using namespace NES;

CoordinatorRPCServer::CoordinatorRPCServer(TopologyPtr topology,
                                           StreamCatalogPtr streamCatalog,
                                           MonitoringManagerPtr monitoringManager)
    : topologyManagerService(std::make_shared<TopologyManagerService>(topology, streamCatalog)),
      streamCatalogService(std::make_shared<StreamCatalogService>(streamCatalog)), monitoringManager(monitoringManager){};

Status CoordinatorRPCServer::RegisterNode(ServerContext*, const RegisterNodeRequest* request, RegisterNodeReply* reply) {
    NES_DEBUG("TopologyManagerService::RegisterNode: request =" << request);
    uint64_t id = topologyManagerService->registerNode(request->address(),
                                                       request->grpcport(),
                                                       request->dataport(),
                                                       request->numberofslots(),
                                                       (NodeType) request->type());

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

Status CoordinatorRPCServer::RegisterPhysicalStream(ServerContext*,
                                                    const RegisterPhysicalStreamRequest* request,
                                                    RegisterPhysicalStreamReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterPhysicalStream: request =" << request);
    TopologyNodePtr physicalNode = this->topologyManagerService->findNodeWithId(request->id());
    bool success = streamCatalogService->registerPhysicalStream(physicalNode,
                                                                request->sourcetype(),
                                                                request->physicalstreamname(),
                                                                request->logicalstreamname());

    if (success) {
        NES_DEBUG("CoordinatorRPCServer::RegisterPhysicalStream success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::RegisterPhysicalStream failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::UnregisterPhysicalStream(ServerContext*,
                                                      const UnregisterPhysicalStreamRequest* request,
                                                      UnregisterPhysicalStreamReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterPhysicalStream: request =" << request);

    TopologyNodePtr physicalNode = this->topologyManagerService->findNodeWithId(request->id());
    bool success =
        streamCatalogService->unregisterPhysicalStream(physicalNode, request->physicalstreamname(), request->logicalstreamname());

    if (success) {
        NES_DEBUG("CoordinatorRPCServer::UnregisterPhysicalStream success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::UnregisterPhysicalStream failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::RegisterLogicalStream(ServerContext*,
                                                   const RegisterLogicalStreamRequest* request,
                                                   RegisterLogicalStreamReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterLogicalStream: request =" << request);

    bool success = streamCatalogService->registerLogicalStream(request->streamname(), request->streamschema());

    if (success) {
        NES_DEBUG("CoordinatorRPCServer::RegisterLogicalStream success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::RegisterLogicalStream failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status CoordinatorRPCServer::UnregisterLogicalStream(ServerContext*,
                                                     const UnregisterLogicalStreamRequest* request,
                                                     UnregisterLogicalStreamReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterLogicalStream: request =" << request);

    bool success = streamCatalogService->unregisterLogicalStream(request->streamname());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::UnregisterLogicalStream success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("CoordinatorRPCServer::UnregisterLogicalStream failed");
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

Status CoordinatorRPCServer::NotifyQueryFailure(ServerContext*, const QueryFailureNotification* request, QueryFailureNotificationReply* reply) {
    try {
        NES_ERROR("CoordinatorRPCServer::notifyQueryFailure: failure message received. id of failed query: " << request->queryid()
                  << "Id of worker: " << request->workerid() << " Reason for failure: " << request->errormsg());
        // TODO implement here what happens with received Query that failed
        reply->set_success(true);
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("CoordinatorRPCServer: received broken failure message: " << ex.what());
        return Status::CANCELLED;
    }
}
