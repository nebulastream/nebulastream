#ifndef NODE_ENGINE_H
#define NODE_ENGINE_H

#include <API/Config.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/NodeProperties.hpp>
#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <iostream>
#include <pthread.h>
#include <string>
#include <unistd.h>
#include <unordered_set>
#include <vector>
#include <zmq.hpp>

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
    enum NodeEngineQueryStatus { started, stopped, registered };
    /**
     * @brief Create a node engine and gather node information
     * and initialize QueryManager, BufferManager and ThreadPool
     */
    NodeEngine();

    ~NodeEngine();

    /**
     * @brief deploy registers and starts a query
     * @param new query plan
     * @return true if succeeded, else false
     */
    bool deployQuery(QueryExecutionPlanPtr qep);

    /**
     * @brief undeploy stops and undeploy a query
     * @param query plan to deploy
     * @return true if succeeded, else false
     */
    bool undeployQuery(QueryExecutionPlanPtr qep);

    /**
     * @brief gregisters a query
     * @param query plan to register
     * @return true if succeeded, else false
     */
    bool registerQuery(QueryExecutionPlanPtr qep);

    /**
     * @brief ungregisters a query
     * @param query plan to unregisterundeployQuery
     * @return true if succeeded, else false
     */
    bool unregisterQuery(QueryExecutionPlanPtr qep);

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
     * @brief start thread pool
     */
    bool start();

    /**
     * @brief stop thread pool
     */
    bool stop();

    JSON getNodePropertiesAsJSON();

    std::string getNodePropertiesAsString();

    NodeProperties* getNodeProperties();

    /**
     * @brief getter of query manager
     * @return query manager
     */
    QueryManagerPtr getQueryManager();

    /**
     * @brief getter of buffer manager
     * @return bufferManager
     */
    BufferManagerPtr getBufferManager();

    /**
     * @brief method to create the buffer manager with defualt config
     * @return bool indicating success
     */
    bool createBufferManager();

    /**
     * @brief method to create buffer manager with custom config
     * @param bufferSize
     * @param numBuffers
     * @return bool indicating success
     */
    bool createBufferManager(size_t bufferSize, size_t numBuffers);

    /**
     * @brief method to stop buffer manager
     * @return bool indicating success
     */
    bool stopBufferManager();

    /**
     * @brief method to start query manager
     * @return bool indicating success
     */
    bool startQueryManager();

    /**
     * @brief method to stop query manager
     * @return bool indicating success
     */
    bool stopQueryManager();

  private:
    NodePropertiesPtr props;
    std::map<QueryExecutionPlanPtr, NodeEngineQueryStatus> queryStatusMap;
    bool forceStop;
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    bool isRunning;
};

typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

} // namespace NES
#endif // NODE_ENGINE_H
