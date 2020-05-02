#include <Actors/ExecutableTransferObject.hpp>
#include <Services/WorkerService.hpp>
#include <Util/Logger.hpp>
#include <Util/SerializationTools.hpp>
#include <utility>

namespace NES {

void WorkerService::shutDown() {
    NES_DEBUG("WorkerService: shutdown WorkerService")
    this->nodeEngine->stop();
    physicalStreams.clear();
}

WorkerService::WorkerService(string ip, uint16_t publish_port, uint16_t receive_port) {
    NES_DEBUG("WorkerService::WorkerService: ip = " << ip)
    this->ip = std::move(ip);
    this->publishPort = publish_port;
    this->receivePort = receive_port;
    NES_DEBUG("WorkerService: create WorkerService with ip=" << this->ip << " publish_port=" << this->publishPort
                                                             << " receive_port=" << this->receivePort)
    physicalStreams.insert(std::make_pair("default_physical", PhysicalStreamConfig()));
    this->nodeEngine = std::make_shared<NodeEngine>();
    this->queryCompiler = createDefaultQueryCompiler(nodeEngine->getQueryManager());
    this->nodeEngine->start();
}

string WorkerService::getNodeProperties() {
    return this->nodeEngine->getNodePropertiesAsString();
}

void WorkerService::addPhysicalStreamConfig(PhysicalStreamConfig conf) {
    physicalStreams.insert(std::make_pair(conf.physicalStreamName, conf));
}

bool WorkerService::registerQuery(const std::string& queryId, std::string& executableTransferObject) {
    NES_DEBUG(
        "WorkerService (" << this->ip << ": registerQueryInNodeEngine " << queryId)
    ExecutableTransferObject eto = SerializationTools::parse_eto(
        executableTransferObject);
    NES_DEBUG(
        "WorkerService eto after parse=" << eto.toString())
    QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan(this->queryCompiler);
    qep->setQueryId(queryId);
    NES_DEBUG("WorkerService add query to queries map")

    bool success = nodeEngine->registerQueryInNodeEngine(qep);
    NES_DEBUG("WorkerService deploy query success=" << success)
    return success;
}

bool WorkerService::unregisterQuery(const std::string& queryId) {
    NES_DEBUG(
        "WorkerService (" << this->ip << ": unregisterQuery " << queryId)

    bool success = nodeEngine->unregisterQuery(queryId);
    NES_DEBUG("WorkerService::unregisterQuery: undeploy success=" << success)

    return success;

}

bool WorkerService::deployQuery(const std::string& queryId, std::string& executableTransferObject) {
    NES_DEBUG(
        "WorkerService (" << this->ip << ": deployQueryInNodeEngine " << queryId)
    ExecutableTransferObject eto = SerializationTools::parse_eto(
        executableTransferObject);
    NES_DEBUG(
        "WorkerService eto after parse=" << eto.toString())
    QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan(this->queryCompiler);
    NES_DEBUG("WorkerService add query to queries map")

    bool success = nodeEngine->deployQueryInNodeEngine(qep);
    NES_DEBUG("WorkerService deploy query success=" << success)
    return success;
}

bool WorkerService::undeployQuery(const std::string& queryId) {
    NES_DEBUG(
        "WorkerService (" << this->ip << ": undeployQuery " << queryId)

    bool success = nodeEngine->undeployQuery(queryId);
    NES_DEBUG("WorkerService::undeployQuery: undeploy success=" << success)

    return success;
}

bool WorkerService::startQuery(const std::string& queryId) {
    NES_DEBUG(
        "WorkerService:startQuery " << queryId)

    bool success = nodeEngine->startQuery(queryId);
    NES_DEBUG("WorkerService::startQuery: start with success =" << success)
    return success;
}

bool WorkerService::stopQuery(const std::string& queryId) {
    NES_DEBUG(
        "WorkerService:stopQuery id=" << queryId)

    bool success = nodeEngine->stopQuery(queryId);
    NES_ERROR("WorkerService::stopQuery: stop with success =" << success)

    return success;
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
