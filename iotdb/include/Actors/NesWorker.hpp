
#ifndef IOTDB_INCLUDE_ACTORS_NESWORKER_HPP_
#define IOTDB_INCLUDE_ACTORS_NESWORKER_HPP_

#include <NodeEngine/NodeEngine.hpp>
#include <Operators/Operator.hpp>
#include <string>
#include <vector>
#include <tuple>

using std::string;
using std::vector;
using std::tuple;

namespace iotdb {

class NesWorker {

 public:
  NesWorker(string ip, uint16_t publish_port, uint16_t receive_port, string sensor_type);
  ~NesWorker() = default;

  /**
   * @brief framework internal method which is called to execute a query or sub-query on a node
   * @param description a description of the query
   * @param executableTransferObject wrapper object with the schema, sources, destinations, operator
   */
  void execute_query(const string &description, string &executableTransferObject);

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
  const string &getSensorType() const;
  void setSensorType(const string &sensor_type);
 private:
  string _ip;
  uint16_t _publish_port;
  uint16_t _receive_port;
  string _sensor_type;

  NodeEngine *_enginePtr;
  std::unordered_map<string, tuple<QueryExecutionPlanPtr, OperatorPtr>> _runningQueries;
};

}

#endif //IOTDB_INCLUDE_ACTORS_NESWORKER_HPP_
