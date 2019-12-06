#ifndef IOTDB_INCLUDE_ACTORS_COORDINATORSERVICE_HPP_
#define IOTDB_INCLUDE_ACTORS_COORDINATORSERVICE_HPP_

#include <SourceSink/DataSource.hpp>
#include <Actors/atom_utils.hpp>
#include <Util/SerializationTools.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>

#include <CodeGen/GeneratedQueryExecutionPlan.hpp>

#include <Topology/FogTopologyManager.hpp>
#include <Optimizer/FogOptimizer.hpp>

#include <API/InputQuery.hpp>
#include <API/UserAPIExpression.hpp>

#include <CodeGen/C_CodeGen/CodeCompiler.hpp>
#include <Util/ErrorHandling.hpp>
#include <CodeGen/CodeGen.hpp>
#include <Operators/Impl/SourceOperator.hpp>
#include <Util/UtilityFunctions.hpp>
#include "Actors/ExecutableTransferObject.hpp"

#include <cstdint>
#include <string>

using std::string;

namespace iotdb {

class CoordinatorService {
 public:
  CoordinatorService() = default;
  ~CoordinatorService() = default;

  /**
   * @brief registers a CAF worker into the NES topology and creates a corresponding FogTopologyWorkerNode object
   * @param ip the worker ip
   * @param publish_port the publish port of the worker
   * @param receive_port the receive port of the worker
   * @param cpu the cpu capacity of the worker
   * @param sensor_type the sensor type which is beeing later on registered as a Stream object
   * @param sap the strong_actor_pointer CAF object to the worker
   */
  FogTopologyEntryPtr register_sensor(const string &ip, uint16_t publish_port, uint16_t receive_port, int cpu,
                                      const string &sensor_type);

  /**
   * @brief deregisters a CAF worker from the NES topology
   * @param entry
   * @return true, if it succeeded, otherwise false
   */
  bool deregister_sensor(const FogTopologyEntryPtr &entry);

  /**
   * @brief registers a CAF query into the NES topology to make it deployable
   * @param description a description of the query
   * @param sensor_type the sensor_type to which the input source stream belongs
   * @param optimizationStrategyName the optimization strategy (buttomUp or topDown)
   */
  FogExecutionPlan register_query(const string &description,
                                  const string &sensor_type,
                                  const string &optimizationStrategyName);

  /**
   * @brief method which is called to unregister an already running query
   * @param description the description of the query
   * @return true if deleted from running queries, otherwise false
   */
  bool deregister_query(const string &description);

  /**
   * @brief deploys a CAF query into the NES topology to the corresponding devices defined by the optimizer
   * @param query a description of the query
   */
  unordered_map<FogTopologyEntryPtr, ExecutableTransferObject> make_deployment(const string &description);

  /**
   * @brief creates a string representation of the topology graph
   * @return the topology as string representation
   */
  string getTopologyPlanString();

  /**
   * @brief register the user query and deploy the user query using a specific deployment strategy
   *
   * @param userQuery
   * @param optimizationStrategyName
   * @return uuid for the user query
   */
  string executeQuery(const string userQuery, const string &optimizationStrategyName);

  const unordered_map<string, tuple<Schema, FogExecutionPlan>> &getRegisteredQueries() const;
  const unordered_map<string, tuple<Schema, FogExecutionPlan>> &getRunningQueries() const;

 private:

  unordered_map<string, int> _queryToPort;
  shared_ptr<FogTopologyManager> _topologyManagerPtr;
  unordered_map<string, tuple<Schema, FogExecutionPlan>> _registeredQueries;
  unordered_map<string, tuple<Schema, FogExecutionPlan>> _runningQueries;

  /**
   * @brief helper method to get all sources in a serialized format from a specific node in the topology
   * @param schema the schema
   * @param v the execution vertex
   * @param execPlan the execution plan
   */
  vector<DataSourcePtr> getSources(const string &description, const ExecutionVertex &v);

  /**
   * @brief helper method to get all sinks in a serialized format from a specific node in the topology
   * @param the descriptor of the query
   * @param schema the schema
   * @param v the execution vertex
   * @param execPlan the execution plan
   */
  vector<DataSinkPtr> getSinks(const string &description, const ExecutionVertex &v);

  /**
   * @brief helper method to get an automatically assigned receive port where ZMQs are communicating.
   * Currently only server/client architecture, i.e., only one layer, is supported
   * @param query the descriptor of the query
   */
  int assign_port(const string &description);
};

}
#endif //IOTDB_INCLUDE_ACTORS_COORDINATORSERVICE_HPP_
