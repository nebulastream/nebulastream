#ifndef IOTDB_INCLUDE_ACTORS_WORKERSERVICE_HPP_
#define IOTDB_INCLUDE_ACTORS_WORKERSERVICE_HPP_

#include <NodeEngine/NodeEngine.hpp>
#include <Operators/Operator.hpp>
#include <string>
#include <vector>
#include <tuple>
#include <Catalogs/PhysicalStreamConfig.hpp>

using std::string;
using std::vector;
using std::tuple;
using JSON = nlohmann::json;

namespace iotdb {

class WorkerService {

 public:
  /**
   * @brief constructor for worker without a stream/sensor attached
   */
  WorkerService(string ip, uint16_t publish_port, uint16_t receive_port);

  /**
   * @brief constructor for worker with a stream/sensor attached
   */
  WorkerService(string ip, uint16_t publish_port, uint16_t receive_port,
               PhysicalStreamConfig streamConf);

  ~WorkerService() = default;

  /**
   * @brief framework internal method which is called to execute a query or sub-query on a node
   * @param queryId a queryId of the query
   * @param executableTransferObject wrapper object with the schema, sources, destinations, operator
   */
  void execute_query(const string &queryId, string &executableTransferObject);

  /**
   * @brief method which is called to unregister an already running query
   * @param query the description of the query
   */
  void delete_query(const string &query);

  /**
   * @brief gets the currently locally running operators and returns them as flattened strings in a vector
   * @return the flattend vector<string> object of operators
   */
  vector<string> getOperators();

  const string &getIp() const;
  void setIp(const string &ip);
  uint16_t getPublishPort() const;
  void setPublishPort(uint16_t publish_port);
  uint16_t getReceivePort() const;
  void setReceivePort(uint16_t receive_port);

  string getNodeProperties();
  PhysicalStreamConfig getPhysicalStreamConfig();

 private:
  string _ip;
  uint16_t _publish_port;
  uint16_t _receive_port;
  PhysicalStreamConfig streamConf;

  NodeEngine *_enginePtr;
  std::unordered_map<string, tuple<QueryExecutionPlanPtr, OperatorPtr>> _runningQueries;
};

}

#endif //IOTDB_INCLUDE_ACTORS_WORKERSERVICE_HPP_
