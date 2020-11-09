/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NODE_ENGINE_H
#define NODE_ENGINE_H

#include <Common/ForwardDeclaration.hpp>
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkManager.hpp>
#include <NodeEngine/NodeStatsProvider.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Plans/Query/QueryId.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <iostream>
#include <pthread.h>
#include <string>
#include <unistd.h>
#include <unordered_set>
#include <vector>
#include <zmq.hpp>

namespace NES {

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class PhysicalStreamConfig;
typedef std::shared_ptr<PhysicalStreamConfig> PhysicalStreamConfigPtr;

/**
 * @brief this class represents the interface and entrance point into the
 * query processing part of NES. It provides basic functionality
 * such as deploying, undeploying, starting, and stopping.
 *
 */
class NodeEngine : public Network::ExchangeProtocolListener, public std::enable_shared_from_this<NodeEngine> {

    static constexpr auto DEFAULT_BUFFER_SIZE = 4096;
    static constexpr auto DEFAULT_NUM_BUFFERS = 1024;

  public:
    enum NodeEngineQueryStatus {
        started,
        stopped,
        registered
    };

    /**
     * @brief this creates a new NodeEngine
     * @param hostname the ip address for the network manager
     * @param port the port for the network manager
     * @param bufferSize the buffer size for the buffer manager
     * @param numBuffers the number of buffers for the buffer manager
     * @return
     */
    static std::shared_ptr<NodeEngine> create(const std::string& hostname, uint16_t port, PhysicalStreamConfigPtr config, size_t bufferSize = DEFAULT_BUFFER_SIZE, size_t numBuffers = DEFAULT_NUM_BUFFERS);

    /**
     * @brief Create a node engine and gather node information
     * and initialize QueryManager, BufferManager and ThreadPool
     */
    explicit NodeEngine(PhysicalStreamConfigPtr config, BufferManagerPtr&&, QueryManagerPtr&&, std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&&,
                        Network::PartitionManagerPtr&&, QueryCompilerPtr&&, uint64_t nodeEngineId);

    ~NodeEngine();

    NodeEngine() = delete;
    NodeEngine(const NodeEngine&) = delete;
    NodeEngine& operator=(const NodeEngine&) = delete;

    /**
     * @brief deploy registers and starts a query
     * @param new query plan
     * @return true if succeeded, else false
     */
    bool deployQueryInNodeEngine(QueryExecutionPlanPtr queryExecutionPlan);

    /**
     * @brief undeploy stops and undeploy a query
     * @param queryId to undeploy
     * @return true if succeeded, else false
     */
    bool undeployQuery(QueryId queryId);

    /**
     * @brief registers a query
     * @param query plan to register
     * @return true if succeeded, else false
     */
    bool registerQueryInNodeEngine(QueryExecutionPlanPtr queryExecutionPlan);

    /**
     * @brief registers a query
     * @param queryId: id of the query sub plan to be registered
     * @param queryExecutionId: query execution plan id
     * @param operatorTree: query sub plan to register
     * @return true if succeeded, else false
     */
    bool registerQueryInNodeEngine(QueryPlanPtr queryPlan);

    /**
     * @brief ungregisters a query
     * @param queryIdto unregister query
     * @return true if succeeded, else false
     */
    bool unregisterQuery(QueryId queryId);

    /**
     * @brief method to start a already deployed query
     * @note if query is not deploy, false is returned
     * @param queryId to start
     * @return bool indicating success
     */
    bool startQuery(QueryId queryId);

    /**
     * @brief method to stop a query
     * @param queryId to stop
     * @return bool indicating success
     */
    bool stopQuery(QueryId queryId);

    /**
     * @brief release all resource of the node engine
     */
    bool stop();

    /**
     * @brief gets the node properties.
     * @return NodePropertiesPtr
     */
    NodeStatsProviderPtr getNodeStatsProvider();

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
     * @brief getter of buffer manager
     * @return bufferManager
     */
    QueryCompilerPtr getCompiler();

    /**
     * @brief getter of network manager
     * @return network manager
     */
    Network::NetworkManagerPtr getNetworkManager();

    /**
     * @return return the status of a query
     */
    QueryExecutionPlan::QueryExecutionPlanStatus getQueryStatus(QueryId queryId);

    /**
    * @brief method to return the query statistics
    * @param id of the query
    * @return vector of queryStatistics
    */
    std::vector<QueryStatisticsPtr> getQueryStatistics(QueryId queryId);

    Network::PartitionManagerPtr getPartitionManager();

    ///// Network Callback //////

    /**
     * @brief this callback is called once a tuple buffer arrives on the network manager
     * for a given nes partition
     */
    void onDataBuffer(Network::NesPartition, TupleBuffer&) override;

    /**
     * @brief this callback is called once an end of stream message arrives
     */
    void onEndOfStream(Network::Messages::EndOfStreamMessage) override;

    /**
     * @brief this callback is called once an error is raised on the server side
     */
    void onServerError(Network::Messages::ErrorMessage) override;

    /**
     * @brief this callback is called once an error is raised on the channel(client) side
     */
    void onChannelError(Network::Messages::ErrorMessage) override;

    /**
     * @brief Set the physical stream config
     * @param config : configuration to be set
     */
    void setConfig(PhysicalStreamConfigPtr config);

  private:
    SourceDescriptorPtr createLogicalSourceDescriptor(SourceDescriptorPtr sourceDescriptor);

    PhysicalStreamConfigPtr config;
    NodeStatsProviderPtr nodeStatsProvider;
    std::map<QueryId, std::vector<QuerySubPlanId>> queryIdToQuerySubPlanIds;
    std::map<QuerySubPlanId, QueryExecutionPlanPtr> deployedQEPs;
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    Network::NetworkManagerPtr networkManager;
    Network::PartitionManagerPtr partitionManager;
    QueryCompilerPtr queryCompiler;
    bool isReleased;
    std::recursive_mutex engineMutex;
    uint64_t nodeEngineId;
};

typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

}// namespace NES
#endif// NODE_ENGINE_H
