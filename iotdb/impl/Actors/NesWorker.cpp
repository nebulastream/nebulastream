#include <Actors/NesWorker.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Util/SerializationTools.hpp>
#include <utility>

namespace iotdb {
NesWorker::NesWorker(string ip, uint16_t publish_port, uint16_t receive_port, string sensor_type) {
  this->_ip = std::move(ip);
  this->_publish_port = publish_port;
  this->_receive_port = receive_port;
  this->_sensor_type = std::move(sensor_type);

  this->_enginePtr = new NodeEngine();
  this->_enginePtr->start();
}

void NesWorker::execute_query(const string &description, string &executableTransferObject) {
  ExecutableTransferObject eto = SerializationTools::parse_eto(executableTransferObject);
  QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan();
  this->_runningQueries.insert({description, std::make_tuple(qep, eto.getOperatorTree())});
  this->_enginePtr->deployQuery(qep);
  std::this_thread::sleep_for(std::chrono::seconds(2));
}

void NesWorker::delete_query(const string &query) {
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

vector<string> NesWorker::getOperators() {
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

const string &NesWorker::getIp() const {
  return _ip;
}
void NesWorker::setIp(const string &ip) {
  _ip = ip;
}
uint16_t NesWorker::getPublishPort() const {
  return _publish_port;
}
void NesWorker::setPublishPort(uint16_t publish_port) {
  _publish_port = publish_port;
}
uint16_t NesWorker::getReceivePort() const {
  return _receive_port;
}
void NesWorker::setReceivePort(uint16_t receive_port) {
  _receive_port = receive_port;
}
const string &NesWorker::getSensorType() const {
  return _sensor_type;
}
void NesWorker::setSensorType(const string &sensor_type) {
  _sensor_type = sensor_type;
}

}


