#ifndef NODE_ENGINE_H
#define NODE_ENGINE_H

#include <API/Config.hpp>
#include <NodeEngine/NodeProperties.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <iostream>
#include <pthread.h>
#include <string>
#include <unistd.h>
#include <unordered_set>
#include <vector>
#include <zmq.hpp>

using namespace std;
namespace NES {
using JSON = nlohmann::json;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

/**
 * @brief this class represents the interface and entrance point into the
 * query processing part of NES. It provides basic functionality
 * such as deploying, undeploying, starting, and stopping.
 *
 */
class NodeEngine {
  public:
    enum NodeEngineQueryStatus {
        started,
        stopped,
        registered
    };
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
    bool deployQueryInNodeEngine(QueryExecutionPlanPtr qep);

    /**
    * @brief deploy registers and starts a query
    * @param queryId
    * @param new query plan as eto
    * @return true if succeeded, else false
    */
    bool deployQueryInNodeEngine(std::string executableTransferObject);

    /**
     * @brief undeploy stops and undeploy a query
     * @param queryId to undeploy
     * @return true if succeeded, else false
     */
    bool undeployQuery(std::string queryId);

    /**
     * @brief registers a query
     * @param query plan to register
     * @return true if succeeded, else false
     */
    bool registerQueryInNodeEngine(QueryExecutionPlanPtr qep);

    /**
    * @brief registers a query
    * @param queryId
    * @param query plan to register as eto
    * @return true if succeeded, else false
    */
    bool registerQueryInNodeEngine(std::string executableTransferObject, OperatorNodePtr operatorTree);

    /**
     * @brief ungregisters a query
     * @param queryIdto unregister query
     * @return true if succeeded, else false
     */
    bool unregisterQuery(std::string queryId);

    /**
     * @brief method to start a already deployed query
     * @note if query is not deploy, false is returned
     * @param queryId to start
     * @return bool indicating success
     */
    bool startQuery(std::string queryId);

    /**
     * @brief method to stop a query
     * @param queryId to stop
     * @return bool indicating success
     */
    bool stopQuery(std::string queryId);

    /**
     * @brief start thread pool
     */
    bool start();

    /**
     * @brief stop thread pool
     * @param force
     */
    bool stop(bool force);

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

    /**
    * @brief method to return the query statistics
    * @param id of the query
    * @return queryStatistics
    */
    QueryStatisticsPtr getQueryStatistics(std::string queryId);

  private:
    NodePropertiesPtr props;
    std::map<QueryExecutionPlanPtr, NodeEngineQueryStatus> qepToStatusMap;
    std::map<std::string, QueryExecutionPlanPtr> queryIdToQepMap;

    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    bool isRunning;

    QueryCompilerPtr queryCompiler;
};

typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

}// namespace NES
#endif// NODE_ENGINE_H
