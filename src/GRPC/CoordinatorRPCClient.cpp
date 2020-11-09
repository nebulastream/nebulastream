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

#include <GRPC/CoordinatorRPCClient.hpp>
#include <NodeStats.pb.h>
#include <Util/Logger.hpp>
#include <boost/filesystem.hpp>
#include <string>
namespace NES {

CoordinatorRPCClient::CoordinatorRPCClient(std::string address)
    : address(address) {
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

CoordinatorRPCClient::~CoordinatorRPCClient() {
    NES_DEBUG("~CoordinatorRPCClient()");
}

bool CoordinatorRPCClient::registerPhysicalStream(PhysicalStreamConfigPtr conf) {
    NES_DEBUG("CoordinatorRPCClient::registerPhysicalStream: got stream config=" << conf->toString() << " workerID=" << workerId);

    RegisterPhysicalStreamRequest request;
    request.set_id(workerId);
    request.set_sourcetype(conf->getSourceType());
    request.set_sourceconf(conf->getSourceConfig());
    request.set_sourcefrequency(conf->getSourceFrequency());
    request.set_numberoftuplestoproduceperbuffer(conf->getNumberOfTuplesToProducePerBuffer());
    request.set_numberofbufferstoproduce(conf->getNumberOfBuffersToProduce());
    request.set_physicalstreamname(conf->getPhysicalStreamName());
    request.set_logicalstreamname(conf->getLogicalStreamName());
    NES_DEBUG("RegisterPhysicalStreamRequest::RegisterLogicalStreamRequest request=" << request.DebugString());

    RegisterPhysicalStreamReply reply;
    ClientContext context;

    Status status = coordinatorStub->RegisterPhysicalStream(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::registerPhysicalStream: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_DEBUG(" CoordinatorRPCClient::registerPhysicalStream error="
                  << status.error_code() << ": "
                  << status.error_message());
        return reply.success();
    }
}

bool CoordinatorRPCClient::registerLogicalStream(std::string streamName,
                                                 std::string filePath) {
    NES_DEBUG("CoordinatorRPCClient: registerLogicalStream " << streamName << " with path" << filePath);

    // Check if file can be found on system and read.
    boost::filesystem::path path{filePath.c_str()};
    if (!boost::filesystem::exists(path)
        || !boost::filesystem::is_regular_file(path)) {
        NES_ERROR("CoordinatorRPCClient: file does not exits");
        throw Exception("files does not exist");
    }

    /* Read file from file system. */
    std::ifstream ifs(path.string().c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)),
                            (std::istreambuf_iterator<char>()));

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
    } else {
        NES_DEBUG(" CoordinatorRPCClient::registerLogicalStream error="
                  << status.error_code() << ": "
                  << status.error_message());
        return reply.success();
    }
}

bool CoordinatorRPCClient::unregisterPhysicalStream(std::string logicalStreamName,
                                                    std::string physicalStreamName) {
    NES_DEBUG(
        "CoordinatorRPCClient: unregisterPhysicalStream physical stream"
        << physicalStreamName << " from logical stream ");

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
    } else {
        NES_DEBUG(" CoordinatorRPCClient::unregisterPhysicalStream error="
                  << status.error_code() << ": "
                  << status.error_message());
        return reply.success();
    }
}

bool CoordinatorRPCClient::unregisterLogicalStream(std::string streamName) {
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
    } else {
        NES_DEBUG(" CoordinatorRPCClient::unregisterLogicalStream error="
                  << status.error_code() << ": "
                  << status.error_message());
        return reply.success();
    }
}

bool CoordinatorRPCClient::addParent(size_t parentId) {
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
    } else {
        NES_DEBUG(" CoordinatorRPCClient::addParent error="
                  << status.error_code() << ": "
                  << status.error_message());
        return reply.success();
    }
}

size_t CoordinatorRPCClient::getId() {
    return workerId;
}

bool CoordinatorRPCClient::removeParent(size_t parentId) {
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
    } else {
        NES_DEBUG(" CoordinatorRPCClient::removeParent error="
                  << status.error_code() << ": "
                  << status.error_message());
        return reply.success();
    }
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
    } else {
        NES_DEBUG(" CoordinatorRPCClient::unregisterNode error="
                  << status.error_code() << ": "
                  << status.error_message());
        return reply.success();
    }
}

bool CoordinatorRPCClient::registerNode(std::string ipAddress, int64_t grpcPort, int64_t dataPort, int16_t numberOfSlots,
                                        NodeType type, NodeStats nodeStats) {
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
    request.mutable_nodeproperties()->CopyFrom(nodeStats);
    request.set_type(type);
    NES_TRACE("CoordinatorRPCClient::RegisterNodeRequest request=" << request.DebugString());

    RegisterNodeReply reply;
    ClientContext context;

    Status status = coordinatorStub->RegisterNode(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::registerNode: status ok id=" << reply.id());
        this->workerId = reply.id();
        return true;
    } else {
        NES_ERROR(" CoordinatorRPCClient::registerNode error="
                  << status.error_code() << ": "
                  << status.error_message());
        return false;
    }
}
}// namespace NES