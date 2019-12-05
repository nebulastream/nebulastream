#include <Services/WorkerService.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Util/SerializationTools.hpp>
#include <utility>

namespace iotdb {
WorkerService::WorkerService(string ip, uint16_t publish_port, uint16_t receive_port, string sensor_type) {
  this->_ip = std::move(ip);
  this->_publish_port = publish_port;
  this->_receive_port = receive_port;
  this->_sensor_type = std::move(sensor_type);

  this->_enginePtr = new NodeEngine();
  this->_enginePtr->start();
}

void WorkerService::execute_query(const string &description, string &executableTransferObject) {
  ExecutableTransferObject eto = SerializationTools::parse_eto(executableTransferObject);
  QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan();
  this->_runningQueries.insert({description, std::make_tuple(qep, eto.getOperatorTree())});
  this->_enginePtr->deployQuery(qep);
  std::this_thread::sleep_for(std::chrono::seconds(2));
}

void WorkerService::delete_query(const string &query) {
  try {
    if (this->_runningQueries.find(query) != this->_runningQueries.end()) {
      QueryExecutionPlanPtr qep = std::get<0>(this->_runningQueries.at(query));
      this->_enginePtr->undeployQuery(qep);
      std::this_thread::sleep_for(std::chrono::seconds(3));
      this->_runningQueries.erase(query);
      IOTDB_INFO(
          "NESWORKER (" << this->_ip << "-" << this->_sensor_type << "): *** Successfully deleted query " << query);
    } else {
      IOTDB_INFO("NESWORKER (" << this->_ip << "-" << this->_sensor_type << "): *** Not found for deletion -> "
                               << query);
    }
  }
  catch (...) {
    // TODO: catch ZMQ termination errors properly
    IOTDB_ERROR("Uncaugth error during deletion!")
  }
}

vector<string> WorkerService::getOperators() {
  vector<string> result;
  for (auto const &x : this->_runningQueries) {
    string str_opts;
    std::set<OperatorType> flattened = std::get<1>(x.second)->flattenedTypes();
    for (const OperatorType &_o: flattened) {
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
const string &WorkerService::getSensorType() const {
  return _sensor_type;
}
void WorkerService::setSensorType(const string &sensor_type) {
  _sensor_type = sensor_type;
}

}


