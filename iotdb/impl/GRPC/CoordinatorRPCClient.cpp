#include <GRPC/CoordinatorRPCClient.hpp>
#include <Util/Logger.hpp>
#include <boost/filesystem.hpp>
#include <string>
namespace NES {

CoordinatorRPCClient::CoordinatorRPCClient(string coordinatorIp, std::string coordinatorPort,
                                           std::string localWorkerIp, std::string localWorkerPort,
                                           size_t numberOfCpus,
                                           NESNodeType type,
                                           string nodeProperties)
    : coordinatorIp(coordinatorIp),
      coordinatorPort(coordinatorPort),
      localWorkerIp(localWorkerIp),
      localWorkerPort(localWorkerPort),
      numberOfCpus(numberOfCpus),
      type(type),
      nodeProperties(nodeProperties) {
    NES_DEBUG("CoordinatorRPCClient::CoordinatorRPCClient");
}

bool CoordinatorRPCClient::registerPhysicalStream(PhysicalStreamConfig conf) {
    NES_DEBUG("CoordinatorRPCClient::registerPhysicalStream: got stream config=" << conf.toString());

    RegisterPhysicalStreamRequest request;
    request.set_id(workerId);
    request.set_sourcetype(conf.sourceType);
    request.set_sourceconf(conf.sourceConfig);
    request.set_sourcefrequency(conf.sourceFrequency);
    request.set_numberofbufferstoproduce(conf.numberOfBuffersToProduce);
    request.set_physicalstreamname(conf.physicalStreamName);
    request.set_logicalstreamname(conf.logicalStreamName);
    NES_DEBUG("RegisterPhysicalStreamRequest::RegisterLogicalStreamRequest request=" << request.DebugString());

    RegisterPhysicalStreamReply reply;

    ClientContext context;

    std::string serverAddress = coordinatorIp + ":" + coordinatorPort;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(serverAddress, grpc::InsecureChannelCredentials());
    std::unique_ptr<CoordinatorRPCService::Stub> coordinatorStub = CoordinatorRPCService::NewStub(chan);
    Status status = coordinatorStub->RegisterPhysicalStream(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::registerPhysicalStream: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_DEBUG(" CoordinatorRPCClient::registerPhysicalStream "
                  "error="
                  << status.error_code() << ": "
                  << status.error_message());
        return reply.success();
    }
}

bool CoordinatorRPCClient::registerLogicalStream(std::string streamName,
                                                 std::string filePath) {
    NES_DEBUG(
        "CoordinatorRPCClient: registerLogicalStream " << streamName << " with path" << filePath);

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

    std::string serverAddress = coordinatorIp + ":" + coordinatorPort;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(serverAddress, grpc::InsecureChannelCredentials());
    std::unique_ptr<CoordinatorRPCService::Stub> coordinatorStub = CoordinatorRPCService::NewStub(chan);
    Status status = coordinatorStub->RegisterLogicalStream(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::registerLogicalStream: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_DEBUG(" CoordinatorRPCClient::registerLogicalStream "
                  "error="
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

    std::string serverAddress = coordinatorIp + ":" + coordinatorPort;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(serverAddress, grpc::InsecureChannelCredentials());
    std::unique_ptr<CoordinatorRPCService::Stub> coordinatorStub = CoordinatorRPCService::NewStub(chan);
    Status status = coordinatorStub->UnregisterPhysicalStream(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::unregisterPhysicalStream: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_DEBUG(" CoordinatorRPCClient::unregisterPhysicalStream "
                  "error="
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

    std::string serverAddress = coordinatorIp + ":" + coordinatorPort;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(serverAddress, grpc::InsecureChannelCredentials());
    std::unique_ptr<CoordinatorRPCService::Stub> coordinatorStub = CoordinatorRPCService::NewStub(chan);
    Status status = coordinatorStub->UnregisterLogicalStream(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::unregisterLogicalStream: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_DEBUG(" CoordinatorRPCClient::unregisterLogicalStream "
                  "error="
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

    std::string serverAddress = coordinatorIp + ":" + coordinatorPort;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(serverAddress, grpc::InsecureChannelCredentials());
    std::unique_ptr<CoordinatorRPCService::Stub> coordinatorStub = CoordinatorRPCService::NewStub(chan);
    Status status = coordinatorStub->AddParent(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::addParent: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_DEBUG(" CoordinatorRPCClient::addParent "
                  "error="
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

    std::string serverAddress = coordinatorIp + ":" + coordinatorPort;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(serverAddress, grpc::InsecureChannelCredentials());
    std::unique_ptr<CoordinatorRPCService::Stub> coordinatorStub = CoordinatorRPCService::NewStub(chan);
    Status status = coordinatorStub->RemoveParent(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::removeParent: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_DEBUG(" CoordinatorRPCClient::removeParent "
                  "error="
                  << status.error_code() << ": "
                  << status.error_message());
        return reply.success();
    }
}

bool CoordinatorRPCClient::registerNode() {
    if (type == NESNodeType::Sensor) {
        NES_DEBUG("CoordinatorRPCClient::registerNode: try to register a sensor");
    } else if (type == NESNodeType::Worker) {
        NES_DEBUG("CoordinatorRPCClient::registerNode: try to register a worker");
    } else {
        NES_ERROR("CoordinatorRPCClient::registerNode node type not supported " << type);
        throw new Exception("CoordinatorRPCClient::registerNode wrong node type");
    }

    std::string address = localWorkerIp + ":" + localWorkerPort;
    RegisterNodeRequest request;
    request.set_address(address);
    request.set_numberofcpus(numberOfCpus);
    //    request.set_nodeproperties(nodeProperties);
    request.set_type(type);
    NES_DEBUG("CoordinatorRPCClient::RegisterNodeRequest request=" << request.DebugString());

    RegisterNodeReply reply;

    ClientContext context;

    std::string serverAddress = coordinatorIp + ":" + coordinatorPort;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(serverAddress, grpc::InsecureChannelCredentials());
    std::unique_ptr<CoordinatorRPCService::Stub> coordinatorStub = CoordinatorRPCService::NewStub(chan);
    Status status = coordinatorStub->RegisterNode(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("CoordinatorRPCClient::registerNode: status ok id=" << reply.id());
        this->workerId = reply.id();
        return true;
    } else {
        NES_ERROR(" CoordinatorRPCClient::registerNode "
                  "error="
                  << status.error_code() << ": "
                  << status.error_message());
        return false;
    }
}

bool CoordinatorRPCClient::connect() {
    NES_DEBUG(
        "CoordinatorRPCClient::connect try to connect to host=" << coordinatorIp << " port=" << coordinatorPort);

    std::string serverAddress = coordinatorIp + ":" + coordinatorPort;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(serverAddress, grpc::InsecureChannelCredentials());
    std::unique_ptr<CoordinatorRPCService::Stub> coordinatorStub = CoordinatorRPCService::NewStub(chan);

    if (chan) {
        NES_DEBUG("CoordinatorRPCClient::connecting: channel successfully created");
        return true;
    } else {
        NES_THROW_RUNTIME_ERROR("CoordinatorRPCClient::connecting error while creating channel");
    }
}

bool CoordinatorRPCClient::disconnecting() {
    NES_WARNING("CoordinatorRPCClient::disconnecting: nothing to do");
}

bool CoordinatorRPCClient::CoordinatorRPCClient::shutdown(bool force) {
    NES_DEBUG("CoordinatorRPCClient: shutdown with force=" << force);
    //TODO: do we have to do something here?
    return true;
}

}// namespace NES