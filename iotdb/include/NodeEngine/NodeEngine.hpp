#ifndef NODE_ENGINE_H
#define NODE_ENGINE_H

#include "NodeProperties.hpp"
#include <CodeGen/QueryExecutionPlan.hpp>
#include <API/Config.hpp>
#include <NodeEngine/ThreadPool.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <iostream>
#include <pthread.h>
#include <string>
#include <unistd.h>
#include <vector>
#include <zmq.hpp>

namespace iotdb {
using JSON = nlohmann::json;

class NodeEngine {
 public:

  /**
   * @brief Create a node engine and gather node information
   * and initialize Dispatcher, BufferManager and ThreadPool
   */
  NodeEngine() {
    props = new NodeProperties();
    init();
  }

  /**
   * @brief deploy a new query plan to via the dispatcher
   * @param new query plan
   */
  void deployQuery(QueryExecutionPlanPtr ptr);

  /**
   * @brief undeploy an existing query plan to via the dispatcher
   * @param query plan to deploy
   */
  void undeployQuery(QueryExecutionPlanPtr qep);

  /**
   * @brief change config of running node eninge
   * @param config with params:
   * numberOfThreads, numberOfBuffers, bufferSize
   */
  void applyConfig(Config& conf);

  /**
   * @brief start thread pool
   */

  void start();
  /**
   * @brief deploy all queries and start thread pool
   */
  void startWithRedeploy();

  /**
   * @brief stop thread pool
   */
  void stop();

  /**
   * @brief undeploy all queries and stop thread pool
   */
  void stopWithUndeploy();

  /**
   * @brief undeploy all queries and delete all qeps
   */
  void resetQEPs();

  JSON getNodePropertiesAsJSON();
  NodeProperties* getNodeProperties();

 private:
  /**
   * @brief initialize Dispatcher, BufferManager and ThreadPool
   */
  void init();

  NodeProperties* props;
  std::vector<QueryExecutionPlanPtr> qeps;

};

typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

}  // namespace iotdb
#endif // NODE_ENGINE_H
