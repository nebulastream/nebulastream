#include <Services/WorkerService.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Util/SerializationTools.hpp>
#include <utility>
#include <Util/Logger.hpp>

namespace NES {

bool WorkerService::shutDown() {
    NES_DEBUG("WorkerService: shutdown WorkerService " << this)
    bool success = this->_enginePtr->stopWithUndeploy();
    if (!success) {
        NES_ERROR("WorkerService::shutDown: error while stopping WorkerService")
        //TODO: we cannot check for this here becuase of the singleton this could already be stopped
        //TODO: after remove singleton we can check this
//        throw new Exception("Error while stopping WorkerService");
    }
    physicalStreams.clear();
    runningQueries.clear();
    return true;
}

WorkerService::WorkerService(string ip, uint16_t publish_port,
                             uint16_t receive_port) {
    NES_DEBUG("WorkerService::WorkerService: ip = " << ip)
    this->ip = std::move(ip);
    this->publishPort = publish_port;
    this->receivePort = receive_port;
    this->queryCompiler = createDefaultQueryCompiler();
    NES_DEBUG("WorkerService: create WorkerService with ip=" << this->ip << " publish_port=" << this->publishPort
                                                             << " receive_port=" << this->receivePort)
    physicalStreams.insert(std::make_pair("default_physical", PhysicalStreamConfig()));
    this->_enginePtr = new NodeEngine();
    this->_enginePtr->start();
}

string WorkerService::getNodeProperties() {
    return this->_enginePtr->getNodePropertiesAsString();
}

PhysicalStreamConfig WorkerService::getPhysicalStreamConfig(std::string name) {
    return physicalStreams[name];
}

void WorkerService::addPhysicalStreamConfig(PhysicalStreamConfig conf) {
    physicalStreams.insert(std::make_pair(conf.physicalStreamName, conf));
}

bool WorkerService::executeQuery(const string& queryId,
                                 string& executableTransferObject) {
    NES_DEBUG(
        "WorkerService (" << this->ip << ": Executing " << queryId)
    ExecutableTransferObject eto = SerializationTools::parse_eto(
        executableTransferObject);
    NES_DEBUG(
        "WorkerService eto after parse=" << eto.toString())
    QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan(this->queryCompiler);
    this->runningQueries.insert(
        {queryId, std::make_tuple(qep, eto.getOperatorTree())});
    this->_enginePtr->deployQuery(qep);
    //TODO: sleeps should be omitted
    //  std::this_thread::sleep_for(std::chrono::seconds(1));
    return true;
}

void WorkerService::deleteQuery(const string& query) {
    try {
        if (this->runningQueries.find(query) != this->runningQueries.end()) {
            NES_DEBUG(
                "WorkerService (" << this->ip << ": Attempting deletion of " << query);
            QueryExecutionPlanPtr qep = std::get<0>(this->runningQueries.at(query));
            this->runningQueries.erase(query);
            this->_enginePtr->undeployQuery(qep);
            NES_INFO(
                "WorkerService (" << this->ip << ": Successfully deleted query " << query);
        } else {
            NES_INFO(
                "WorkerService (" << this->ip << ": *** Not found for deletion -> " << query);
        }
    } catch (...) {
        // TODO: catch ZMQ termination errors properly
        NES_ERROR(
            "WorkerService (" << this->ip << "): Undefined error during deletion!")
    }
}

vector<string> WorkerService::getOperators() {
    vector<string> result;
    for (auto const& x : this->runningQueries) {
        string str_opts;
        const OperatorPtr op = std::get<1>(x.second);
        auto flattened = op->flattenedTypes();
        for (const OperatorType& _o : flattened) {
            if (!str_opts.empty())
                str_opts.append(", ");
            str_opts.append(operatorTypeToString.at(_o));
        }
        result.emplace_back(x.first + "->" + str_opts);
    }
    return result;
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

}

