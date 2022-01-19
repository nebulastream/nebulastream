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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <Monitoring/MetricValues/StaticNesMetrics.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <filesystem>
#include <fstream>
#include <string>

namespace NES {

CoordinatorRPCClient::CoordinatorRPCClient(const std::string& address) : address(address) {
    NES_DEBUG("CoordinatorRPCClient(): creating channels to address =" << address);
    rpcChannel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());

    if (rpcChannel) {
        NES_DEBUG("CoordinatorRPCClient::connecting: channel successfully created");
    } else {
        NES_THROW_RUNTIME_ERROR("CoordinatorRPCClient::connecting error while creating channel");
    }
    coordinatorStub = CoordinatorRPCService::NewStub(rpcChannel);
    workerId = SIZE_MAX;
}

bool CoordinatorRPCClient::registerPhysicalStream(const AbstractPhysicalStreamConfigPtr& conf) {
    NES_DEBUG("CoordinatorRPCClient::registerPhysicalStream: got stream config=" << conf->toString() << " workerID=" << workerId);

    RegisterPhysicalStreamRequest request;
    request.set_id(workerId);
    request.set_sourcetype(
        conf->getPhysicalStreamTypeConfig()->getPhysicalStreamTypeConfiguration()->getSourceType()->getValue());
    request.set_physicalstreamname(conf->getPhysicalStreamTypeConfig()->getPhysicalStreamName()->getValue());
    request.set_logicalstreamname(conf->getPhysicalStreamTypeConfig()->getLogicalStreamName()->getValue());
    NES_DEBUG("RegisterPhysicalStreamRequest::RegisterLogicalStreamRequest request=" << request.DebugString());

    RegisterPhysicalStreamReply reply;
    ClientContext context;

    Status status = coordinatorStub->RegisterPhysicalStream(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::registerPhysicalStream: status ok return success=" << reply.success());
        return reply.success();
    }
    NES_DEBUG(" CoordinatorRPCClient::registerPhysicalStream error=" << status.error_code() << ": " << status.error_message());
    return reply.success();
}

bool CoordinatorRPCClient::registerLogicalStream(const std::string& streamName, const std::string& filePath) {
    NES_DEBUG("CoordinatorRPCClient: registerLogicalStream " << streamName << " with path" << filePath);

    // Check if file can be found on system and read.
    std::filesystem::path path{filePath.c_str()};
    if (!std::filesystem::exists(path) || !std::filesystem::is_regular_file(path)) {
        NES_ERROR("CoordinatorRPCClient: file does not exits");
        throw Exception("files does not exist");
    }

    /* Read file from file system. */
    std::ifstream ifs(path.string().c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    RegisterLogicalStreamRequest request;
    request.set_id(workerId);
    request.set_streamname(streamName);
    request.set_streamschema(fileContent);
    NES_DEBUG("CoordinatorRPCClient::RegisterLogicalStreamRequest request=" << request.DebugString());

    RegisterLogicalStreamReply reply;
    ClientContext context;

    Status status = coordinatorStub->RegisterLogicalStream(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::registerLogicalStream: status ok return success=" << reply.success());
        return reply.success();
    }
    NES_DEBUG(" CoordinatorRPCClient::registerLogicalStream error=" << status.error_code() << ": " << status.error_message());
    return reply.success();
}

bool CoordinatorRPCClient::unregisterPhysicalStream(const std::string& logicalStreamName, const std::string& physicalStreamName) {
    NES_DEBUG("CoordinatorRPCClient: unregisterPhysicalStream physical stream" << physicalStreamName << " from logical stream ");

    UnregisterPhysicalStreamRequest request;
    request.set_id(workerId);
    request.set_physicalstreamname(physicalStreamName);
    request.set_logicalstreamname(logicalStreamName);
    NES_DEBUG("CoordinatorRPCClient::UnregisterPhysicalStreamRequest request=" << request.DebugString());

    UnregisterPhysicalStreamReply reply;
    ClientContext context;

    Status status = coordinatorStub->UnregisterPhysicalStream(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::unregisterPhysicalStream: status ok return success=" << reply.success());
        return reply.success();
    }
    NES_DEBUG(" CoordinatorRPCClient::unregisterPhysicalStream error=" << status.error_code() << ": " << status.error_message());
    return reply.success();
}

bool CoordinatorRPCClient::unregisterLogicalStream(const std::string& streamName) {
    NES_DEBUG("CoordinatorRPCClient: unregisterLogicalStream stream" << streamName);

    UnregisterLogicalStreamRequest request;
    request.set_id(workerId);
    request.set_streamname(streamName);
    NES_DEBUG("CoordinatorRPCClient::UnregisterLogicalStreamRequest request=" << request.DebugString());

    UnregisterLogicalStreamReply reply;
    ClientContext context;

    Status status = coordinatorStub->UnregisterLogicalStream(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::unregisterLogicalStream: status ok return success=" << reply.success());
        return reply.success();
    }
    NES_DEBUG(" CoordinatorRPCClient::unregisterLogicalStream error=" << status.error_code() << ": " << status.error_message());
    return reply.success();
}

bool CoordinatorRPCClient::addParent(uint64_t parentId) {
    NES_DEBUG("CoordinatorRPCClient: addParent parentId=" << parentId << " workerId=" << workerId);

    AddParentRequest request;
    request.set_parentid(parentId);
    request.set_childid(workerId);
    NES_DEBUG("CoordinatorRPCClient::AddParentRequest request=" << request.DebugString());

    AddParentReply reply;
    ClientContext context;

    Status status = coordinatorStub->AddParent(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::addParent: status ok return success=" << reply.success());
        return reply.success();
    }
    NES_DEBUG(" CoordinatorRPCClient::addParent error=" << status.error_code() << ": " << status.error_message());
    return reply.success();
}

bool CoordinatorRPCClient::replaceParent(uint64_t oldParentId, uint64_t newParentId) {
    NES_DEBUG("CoordinatorRPCClient: replaceParent oldParentId=" << oldParentId << " newParentId=" << newParentId
                                                                 << " workerId=" << workerId);

    ReplaceParentRequest request;
    request.set_childid(workerId);
    request.set_oldparent(oldParentId);
    request.set_newparent(newParentId);
    NES_DEBUG("CoordinatorRPCClient::replaceParent request=" << request.DebugString());

    ReplaceParentReply reply;
    ClientContext context;

    Status status = coordinatorStub->ReplaceParent(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::replaceParent: status ok return success=" << reply.success());
        return reply.success();
    }
    NES_DEBUG(" CoordinatorRPCClient::replaceParent error=" << status.error_code() << ": " << status.error_message());
    return reply.success();
}

uint64_t CoordinatorRPCClient::getId() const { return workerId; }

bool CoordinatorRPCClient::removeParent(uint64_t parentId) {
    NES_DEBUG("CoordinatorRPCClient: removeParent parentId" << parentId << " workerId=" << workerId);

    RemoveParentRequest request;
    request.set_parentid(parentId);
    request.set_childid(workerId);
    NES_DEBUG("CoordinatorRPCClient::RemoveParentRequest request=" << request.DebugString());

    RemoveParentReply reply;
    ClientContext context;

    Status status = coordinatorStub->RemoveParent(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::removeParent: status ok return success=" << reply.success());
        return reply.success();
    }
    NES_DEBUG(" CoordinatorRPCClient::removeParent error=" << status.error_code() << ": " << status.error_message());
    return reply.success();
}

bool CoordinatorRPCClient::unregisterNode() {
    NES_DEBUG("CoordinatorRPCClient::unregisterNode workerId=" << workerId);

    UnregisterNodeRequest request;
    request.set_id(workerId);
    NES_DEBUG("CoordinatorRPCClient::unregisterNode request=" << request.DebugString());

    UnregisterNodeReply reply;
    ClientContext context;

    Status status = coordinatorStub->UnregisterNode(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::unregisterNode: status ok return success=" << reply.success());
        return reply.success();
    }
    NES_DEBUG(" CoordinatorRPCClient::unregisterNode error=" << status.error_code() << ": " << status.error_message());
    return reply.success();
}

bool CoordinatorRPCClient::registerNode(const std::string& ipAddress,
                                        int64_t grpcPort,
                                        int64_t dataPort,
                                        int16_t numberOfSlots,
                                        NodeType type,
                                        std::optional<StaticNesMetricsPtr> staticNesMetrics) {
    if (type == NodeType::Sensor) {
        NES_DEBUG("CoordinatorRPCClient::registerNode: try to register a sensor workerID=" << workerId);
    } else if (type == NodeType::Worker) {
        NES_DEBUG("CoordinatorRPCClient::registerNode: try to register a worker");
    } else {
        NES_ERROR("CoordinatorRPCClient::registerNode node type not supported " << type);
        throw Exception("CoordinatorRPCClient::registerNode wrong node type");
    }

    RegisterNodeRequest request;
    request.set_address(ipAddress);
    request.set_grpcport(grpcPort);
    request.set_dataport(dataPort);
    request.set_numberofslots(numberOfSlots);
    request.set_type(type);

    if (staticNesMetrics.has_value()) {
        request.mutable_monitoringdata()->Swap(staticNesMetrics.value()->toProtobufSerializable().get());
    } else {
        NES_WARNING("CoordinatorRPCClient: Registering node without monitoring data.");
    }

    NES_TRACE("CoordinatorRPCClient::RegisterNodeRequest request=" << request.DebugString());

    RegisterNodeReply reply;
    ClientContext context;

    Status status = coordinatorStub->RegisterNode(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::registerNode: status ok id=" << reply.id());
        this->workerId = reply.id();
        return true;
    }
    NES_ERROR(" CoordinatorRPCClient::registerNode error=" << status.error_code() << ": " << status.error_message());
    return false;
}

bool CoordinatorRPCClient::notifyQueryFailure(uint64_t queryId,
                                              uint64_t subQueryId,
                                              uint64_t workerId,
                                              uint64_t operatorId,
                                              std::string errorMsg) {

    // create & fill the protobuf
    QueryFailureNotification request;
    request.set_queryid(queryId);
    request.set_subqueryid(subQueryId);
    request.set_workerid(workerId);
    request.set_operatorid(operatorId);
    request.set_errormsg(errorMsg);

    QueryFailureNotificationReply reply;

    ClientContext context;

    Status status = coordinatorStub->NotifyQueryFailure(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::NotifyQueryFailure: status ok");
        return true;
    }
    return false;
}

}// namespace NES