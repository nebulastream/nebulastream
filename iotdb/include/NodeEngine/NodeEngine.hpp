#ifndef NODE_ENGINE_H
#define NODE_ENGINE_H

#include <NodeEngine/NodeProperties.hpp>
#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <API/Config.hpp>
#include <NodeEngine/ThreadPool.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <iostream>
#include <pthread.h>
#include <string>
#include <unistd.h>
#include <vector>
#include <zmq.hpp>
#include <unordered_set>

namespace NES {
using JSON = nlohmann::json;

/**
 * @brief this class represents the interface and entrance point into the
 * query processing part of NES. It provides basic functionality
 * such as deploying, undeploying, starting, and stoping.
 *
 */
class NodeEngine {
  public:

    /**
     * @brief Create a node engine and gather node information
     * and initialize Dispatcher, BufferManager and ThreadPool
     */
    NodeEngine() {
        props = std::make_shared<NodeProperties>();
        init();
    }

    ~NodeEngine() {
        stop();
    }

    /**
     * @brief deploy a new query plan to via the dispatcher
     * @param new query plan
     * @return true if succeeded, else false
     */
    bool deployQuery (QueryExecutionPlanPtr qep);

    /**
   * @brief undeploy an existing query plan to via the dispatcher
   * @param query plan to deploy
   * @return true if succeeded, else false
   */
    bool undeployQuery(QueryExecutionPlanPtr qep);

    /**
    * @brief method to start a already deployed query
    * @note if query is not deploy, false is returned
    * @param qep to start
    * @return bool indicating success
    */
    bool startQuery(QueryExecutionPlanPtr qep);

    /**
     * @brief method to stop a query
     * @param qep to stop
     * @return bool indicating success
     */
    bool stopQuery(QueryExecutionPlanPtr qPtr);

    /**
     * @brief change config of running node eninge
     * @param config with params:
     * numberOfThreads, numberOfBuffers, bufferSize
     */
    void applyConfig(Config& conf);

    /**
     * @brief start thread pool
     */
    bool start();

    /**
     * @brief deploy all queries and start thread pool
     */
    bool startWithRedeploy();

    /**
     * @brief stop thread pool
     */
    bool stop();

    /**
     * @brief undeploy all queries and stop thread pool
     */
    bool stopWithUndeploy();

    /**
     * @brief undeploy all queries and delete all qeps
     */
    void resetQEPs();

    JSON getNodePropertiesAsJSON();

    std::string getNodePropertiesAsString();

    NodeProperties* getNodeProperties();

    void setDOPWithRestart(size_t dop);
    void setDOPWithoutRestart(size_t dop);

    size_t getDOP();

  private:
    /**
     * @brief initialize Dispatcher, BufferManager and ThreadPool
     */
    void init();

    NodePropertiesPtr props;
    std::unordered_set<QueryExecutionPlanPtr> qeps;
};

typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

}  // namespace NES
#endif // NODE_ENGINE_H
