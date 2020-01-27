#include <Services/WorkerService.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Util/SerializationTools.hpp>
#include <utility>
#include <Util/Logger.hpp>

namespace NES {
WorkerService::WorkerService(string ip, uint16_t publish_port,
                             uint16_t receive_port) {
  this->_ip = std::move(ip);
  this->_publish_port = publish_port;
  this->_receive_port = receive_port;
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

void WorkerService::addPhysicalStreamConfig(PhysicalStreamConfig conf)
{
  physicalStreams.insert(std::make_pair(conf.physicalStreamName, conf));
}

void WorkerService::execute_query(const string &queryId,
                                  string &executableTransferObject) {
  NES_DEBUG(
      "WORKERSERVICE (" << this->_ip << ": Executing " << queryId);
  ExecutableTransferObject eto = SerializationTools::parse_eto(
      executableTransferObject);
  QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan();
  this->_runningQueries.insert(
      { queryId, std::make_tuple(qep, eto.getOperatorTree()) });
  this->_enginePtr->deployQuery(qep);
  std::this_thread::sleep_for(std::chrono::seconds(2));
}

void WorkerService::delete_query(const string &query) {
  try {
    if (this->_runningQueries.find(query) != this->_runningQueries.end()) {
      NES_DEBUG(
          "WORKERSERVICE (" << this->_ip << ": Attempting deletion of " << query);
      QueryExecutionPlanPtr qep = std::get<0>(this->_runningQueries.at(query));
      this->_runningQueries.erase(query);
      this->_enginePtr->undeployQuery(qep);
      NES_INFO(
          "WORKERSERVICE (" << this->_ip << ": Successfully deleted query " << query);
    } else {
      NES_INFO(
          "WORKERSERVICE (" << this->_ip << ": *** Not found for deletion -> " << query);
    }
  } catch (...) {
    // TODO: catch ZMQ termination errors properly
    NES_ERROR(
        "WORKERSERVICE (" << this->_ip << "): Undefined error during deletion!")
  }
}

vector<string> WorkerService::getOperators() {
  vector<string> result;
  for (auto const &x : this->_runningQueries) {
    string str_opts;
    const OperatorPtr& op = std::get<1>(x.second);
    auto flattened = op->flattenedTypes();
    for (const OperatorType &_o : flattened) {
      if (!str_opts.empty())
        str_opts.append(", ");
      str_opts.append(operatorTypeToString.at(_o));
    }
    result.emplace_back(x.first + "->" + str_opts);
  }
  return result;
}

const string &WorkerService::getIp() const {
  return _ip;
}
void WorkerService::setIp(const string &ip) {
  _ip = ip;
}
uint16_t WorkerService::getPublishPort() const {
  return _publish_port;
}
void WorkerService::setPublishPort(uint16_t publish_port) {
  _publish_port = publish_port;
}
uint16_t WorkerService::getReceivePort() const {
  return _receive_port;
}
void WorkerService::setReceivePort(uint16_t receive_port) {
  _receive_port = receive_port;
}

}

