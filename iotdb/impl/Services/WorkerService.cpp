#include <Actors/ExecutableTransferObject.hpp>
#include <Services/WorkerService.hpp>
#include <Util/Logger.hpp>
#include <Util/SerializationTools.hpp>
#include <utility>

namespace NES {

void WorkerService::shutDown() {
    NES_DEBUG("WorkerService: shutdown WorkerService")
    this->nodeEngine->stopWithUndeploy();
    physicalStreams.clear();
    queryIdToQEPMap.clear();
    queryIdToStatusMap.clear();
}

WorkerService::WorkerService(string ip, uint16_t publish_port, uint16_t receive_port) {
    NES_DEBUG("WorkerService::WorkerService: ip = " << ip)
    this->ip = std::move(ip);
    this->publishPort = publish_port;
    this->receivePort = receive_port;
    this->queryCompiler = createDefaultQueryCompiler();
    NES_DEBUG("WorkerService: create WorkerService with ip=" << this->ip << " publish_port=" << this->publishPort
                                                             << " receive_port=" << this->receivePort)
    physicalStreams.insert(std::make_pair("default_physical", PhysicalStreamConfig()));
    this->nodeEngine = std::make_shared<NodeEngine>();
    this->nodeEngine->start();
}

string WorkerService::getNodeProperties() {
    return this->nodeEngine->getNodePropertiesAsString();
}

void WorkerService::addPhysicalStreamConfig(PhysicalStreamConfig conf) {
    physicalStreams.insert(std::make_pair(conf.physicalStreamName, conf));
}

bool WorkerService::deployQuery(const std::string& queryId, std::string& executableTransferObject) {
    NES_DEBUG(
        "WorkerService (" << this->ip << ": deployQuery " << queryId)
    ExecutableTransferObject eto = SerializationTools::parse_eto(
        executableTransferObject);
    NES_DEBUG(
        "WorkerService eto after parse=" << eto.toString())
    QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan(this->queryCompiler);
    NES_DEBUG("WorkerService add query to queries map")
    queryIdToQEPMap.insert({queryId, qep});
    queryIdToStatusMap.insert({queryId, NodeQueryStatus::deployed});

    bool success = nodeEngine->deployQuery(qep);
    NES_DEBUG("WorkerService deploy query success=" << success)
    return success;
}

bool WorkerService::undeployQuery(const std::string& queryId) {
    NES_DEBUG(
        "WorkerService (" << this->ip << ": undeployQuery " << queryId)

    if (queryIdToStatusMap[queryId] != NodeQueryStatus::stopped) {
        NES_ERROR(
            "WorkerService::undeployQuery: cannot undeploy because not stopped status=" << queryIdToStatusMap[queryId])
        return false;
    } else {
        NES_DEBUG("WorkerService::undeployQuery: query found and status stopped")
    }

    if (queryIdToQEPMap.count(queryId) == 0) {
        NES_ERROR(
            "WorkerService::undeployQuery: cannot find QEP for qep=" << queryId)
        return false;
    }

    bool success = nodeEngine->undeployQuery(queryIdToQEPMap[queryId]);
    NES_DEBUG("WorkerService::undeployQuery: undeploy success=" << success)

    size_t delIdQepMap = queryIdToQEPMap.erase(queryId);
    size_t delIdStatusMap = queryIdToStatusMap.erase(queryId);

    if (delIdQepMap == 1 && delIdStatusMap == 1) {
        NES_DEBUG("WorkerService::undeployQuery: query successfully erased")
        return true;
    } else if (delIdQepMap != 1) {
        NES_ERROR("WorkerService::undeployQuery: erase in delIdQepMap failed as return is " << delIdQepMap)
        return false;
    } else {
        NES_ERROR("WorkerService::undeployQuery: erase in delIdStatusMap failed as return is " << delIdStatusMap)
        return false;
    }
}

bool WorkerService::startQuery(const std::string& queryId) {
    NES_DEBUG(
        "WorkerService:startQuery " << queryId)

    if (queryIdToStatusMap[queryId] == NodeQueryStatus::started) {
        NES_ERROR("WorkerService::startQuery: query already started")
        return false;
    } else {
        NES_DEBUG("WorkerService::startQuery: of status=" << queryIdToStatusMap[queryId])
        bool success = nodeEngine->startQuery(queryIdToQEPMap[queryId]);
        NES_ERROR("WorkerService::startQuery: start with success =" << success)
        queryIdToStatusMap[queryId] = NodeQueryStatus::started;
        return success;
    }
}

bool WorkerService::stopQuery(const std::string& queryId) {
    NES_DEBUG(
        "WorkerService:stopQuery " << queryId)

    if (queryIdToStatusMap[queryId] != NodeQueryStatus::started) {
        NES_ERROR("WorkerService::stopQuery: query start fail because in status " << queryIdToStatusMap[queryId])
        return false;
    } else {
        NES_DEBUG("WorkerService::stopQuery =" << queryIdToStatusMap[queryId])
        bool success = nodeEngine->stopQuery(queryIdToQEPMap[queryId]);
        NES_ERROR("WorkerService::stopQuery: start with success =" << success)
        queryIdToStatusMap[queryId] = NodeQueryStatus::stopped;
        return success;
    }
}

string& WorkerService::getIp() {
    return ip;
}

void WorkerService::setIp(const string& ip) {
    this->ip = ip;
}

uint16_t WorkerService::getPublishPort() const {
    return publishPort;
}
void WorkerService::setPublishPort(uint16_t publish_port) {
    publishPort = publish_port;
}
uint16_t WorkerService::getReceivePort() const {
    return receivePort;
}
void WorkerService::setReceivePort(uint16_t receive_port) {
    receivePort = receive_port;
}

} // namespace NES
