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

#include <Catalogs/AbstractPhysicalStreamConfig.hpp>
#include <Common/ForwardDeclaration.hpp>
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkManager.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Runtime/ErrorListener.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <Runtime/NodeStatsProvider.hpp>
#include <Runtime/QueryManager.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>
#include <iostream>
#include <pthread.h>
#include <string>
#include <unistd.h>
#include <unordered_set>
#include <vector>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class QueryReconfigurationPlan;
typedef std::shared_ptr<QueryReconfigurationPlan> QueryReconfigurationPlanPtr;

class PhysicalStreamConfig;
using PhysicalStreamConfigPtr = std::shared_ptr<PhysicalStreamConfig>;

}// namespace NES

namespace NES::Runtime {

NodeEnginePtr create(const std::string& hostname, uint16_t port, PhysicalStreamConfigPtr config);

/**
 * @brief this class represents the interface and entrance point into the
 * query processing part of NES. It provides basic functionality
 * such as deploying, undeploying, starting, and stopping.
 *
 */
class NodeEngine : public Network::ExchangeProtocolListener,
                   public NES::detail::virtual_enable_shared_from_this<NodeEngine>,
                   public ErrorListener {
    // virtual_enable_shared_from_this necessary for double inheritance of enable_shared_from_this
    using inherited0 = Network::ExchangeProtocolListener;
    using inherited1 = virtual_enable_shared_from_this<NodeEngine>;
    using inherited2 = ErrorListener;

  public:
    enum NodeEngineQueryStatus { started, stopped, registered };

    /**
     * @brief this creates a new Runtime
     * @param hostname the ip address for the network manager
     * @param port the port for the network manager
     * @param numThreads the number of worker threads for this nodeEngine
     * @param bufferSize the buffer size for the buffer manager
     * @param numBuffers the number of buffers for the buffer manager
     * @return
     */

    static NodeEnginePtr create(const std::string& hostname,
                                uint16_t port,
                                const PhysicalStreamConfigPtr& config,
                                uint16_t numThreads,
                                uint64_t bufferSize,
                                uint64_t numberOfBuffersInGlobalBufferManager,
                                uint64_t numberOfBuffersInSourceLocalBufferPool,
                                uint64_t numberOfBuffersPerPipeline);
    /**
     * @brief Create a node engine and gather node information
     * and initialize QueryManager, BufferManager and ThreadPool
     */
    explicit NodeEngine(const PhysicalStreamConfigPtr& config,
                        BufferManagerPtr&&,
                        QueryManagerPtr&&,
                        std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&&,
                        Network::PartitionManagerPtr&&,
                        QueryCompilation::QueryCompilerPtr&&,
                        StateManagerPtr&&,
                        uint64_t nodeEngineId,
                        uint64_t numberOfBuffersInGlobalBufferManager,
                        uint64_t numberOfBuffersInSourceLocalBufferPool,
                        uint64_t numberOfBuffersPerPipeline);

    ~NodeEngine() override;

    NodeEngine() = delete;
    NodeEngine(const NodeEngine&) = delete;
    NodeEngine& operator=(const NodeEngine&) = delete;

    /**
     * @brief signal handler: behaviour not clear yet!
     * @param signalNumber
     * @param callstack
     */
    void onFatalError(int signalNumber, std::string callstack) override;

    /**
     * @brief exception handler: behaviour not clear yet!
     * @param exception
     * @param callstack
     */
    void onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) override;

    /**
     * @brief deploy registers and starts a query
     * @param new query plan
     * @return true if succeeded, else false
     */
    bool deployQueryInNodeEngine(const Execution::ExecutableQueryPlanPtr& queryExecutionPlan);

    /**
     * @brief undeploy stops and undeploy a query
     * @param queryId to undeploy
     * @return true if succeeded, else false
     */
    bool undeployQuery(QueryId queryId);

    /**
     * @brief registers a query
     * @param query plan to register
     * @param lazyQueryManagerRegistration if true, postpones query manager to caller
     * @return true if succeeded, else false
     */
    bool registerQueryInNodeEngine(const Execution::ExecutableQueryPlanPtr& queryExecutionPlan,
                                   bool lazyQueryManagerRegistration = false);

    /**
     * @brief registers a query
     * @param queryId: id of the query sub plan to be registered
     * @param queryExecutionId: query execution plan id
     * @param operatorTree: query sub plan to register
     * @return true if succeeded, else false
     */
    bool registerQueryInNodeEngine(const QueryPlanPtr& queryPlan);

    /**
     * @brief registers a query for reconfiguration process to start
     * @param queryPlan: query sub plan to be registered
     * @return true if succeeded, else false
     */
    bool registerQueryForReconfigurationInNodeEngine(QueryPlanPtr queryPlan);

    /**
     * Method to start reconfiguration Process
     * @param queryReconfigurationPlan to reconfigure
     * @return true if succeeded, else false
     */
    bool startQueryReconfiguration(QueryId queryId, QueryReconfigurationPlanPtr queryReconfigurationPlan);

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
     * @param graceful hard or soft termination
     * @return bool indicating success
     */
    bool stopQuery(QueryId queryId, bool graceful = false);

    /**
     * @brief release all resource of the node engine
     * @param withError true if the node engine stopped with an error
     */
    bool stop(bool markQueriesAsFailed = false);

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
    * @brief getter of state manager
    * @return stateManager
    */
    StateManagerPtr getStateManager();

    /**
    * @brief getter of node id
    * @return NodeEngineId
    */
    uint64_t getNodeEngineId();

    /**
     * @brief getter of buffer manager
     * @return bufferManager
     */
    QueryCompilation::QueryCompilerPtr getCompiler();

    /**
     * @brief getter of network manager
     * @return network manager
     */
    Network::NetworkManagerPtr getNetworkManager();

    /**
     * @return return the status of a query
     */
    Execution::ExecutableQueryPlanStatus getQueryStatus(QueryId queryId);

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
     * @brief this callback is called once a client announcement message arrives at network manager
     * When partition is registered and not in pendingRegistration then increment producer. otherwise if present in partitionsWaitingRegistration then
     * partition will be registered lazily after channel is established. Otherwise error.
     */
    bool onClientAnnouncement(Network::Messages::ClientAnnounceMessage msg) override;

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
    * @brief this callback is called once an reconfiguration is triggered
    */
    void onQueryReconfiguration(Network::ChannelId channelId, QueryReconfigurationPlanPtr queryReconfigurationPlan) override;

    // TODO we should get rid of the following method
    /**
     * @brief Set the physical stream config
     * @param config : configuration to be set
     */
    void setConfig(const AbstractPhysicalStreamConfigPtr& config);

    /**
     * @brief Creates a logical source descriptor according to a logical source descriptor
     * TODO place in proper catalog
     * @param sourceDescriptor
     * @return
     */
    SourceDescriptorPtr createLogicalSourceDescriptor(const SourceDescriptorPtr& sourceDescriptor);

  private:
    /**
     * Compile Query Plan into executable query plan
     * @param queryPlan to compile to Executable Query Plan
     * @return Executable Plan if compilation successful else nullopt
     */
    std::optional<Execution::ExecutableQueryPlanPtr> compileQuery(QueryPlanPtr queryPlan);
    bool registerQueryExecutionPlanInQueryManagerForSources(Execution::ExecutableQueryPlanPtr& queryExecutionPlan,
                                                            const std::vector<DataSourcePtr>& sources);

    std::vector<AbstractPhysicalStreamConfigPtr> configs;
    NodeStatsProviderPtr nodeStatsProvider;
    std::map<OperatorId, std::vector<Execution::SuccessorExecutablePipeline>> sourceIdToSuccessorExecutablePipeline;
    std::map<QueryId, std::vector<QuerySubPlanId>> queryIdToQuerySubPlanIds;
    std::map<QuerySubPlanId, Execution::ExecutableQueryPlanPtr> deployedQEPs;
    std::map<QuerySubPlanId, Execution::ExecutableQueryPlanPtr> reconfigurationQEPs;
    std::unordered_set<Network::NesPartition> partitionsWaitingRegistration;
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    Network::NetworkManagerPtr networkManager;
    Network::PartitionManagerPtr partitionManager;
    StateManagerPtr stateManager;
    QueryCompilation::QueryCompilerPtr queryCompiler;
    std::atomic<bool> isRunning{};
    mutable std::recursive_mutex engineMutex;
    [[maybe_unused]] uint64_t nodeEngineId;
    [[maybe_unused]] uint32_t numberOfBuffersInGlobalBufferManager;
    [[maybe_unused]] uint32_t numberOfBuffersInSourceLocalBufferPool;
    [[maybe_unused]] uint32_t numberOfBuffersPerPipeline;
    void reconfigurationStartSequence(QueryReconfigurationPlanPtr queryReconfigurationPlan,
                                      Network::NesPartition& partition,
                                      QuerySubPlanId querySubPlanId);
};

using NodeEnginePtr = std::shared_ptr<NodeEngine>;

}// namespace NES::Runtime
#endif// NODE_ENGINE_H
