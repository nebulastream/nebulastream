/*
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
#ifndef NES_TOPOLOGYCHANGEREQUEST_HPP
#define NES_TOPOLOGYCHANGEREQUEST_HPP
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <RequestProcessor/RequestTypes/AbstractUniRequest.hpp>

namespace NES {
class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

namespace RequestProcessor::Experimental {
class TopologyChangeRequest;
using TopologyChangeRequestPtr = std::shared_ptr<TopologyChangeRequest>;

struct TopologyChangeRequestResponse : AbstractRequestResponse {
    explicit TopologyChangeRequestResponse(bool success) : success(success) {};
    bool success;
};

class TopologyChangeRequest : public AbstractUniRequest {
  public:
    /**
     * @brief Constructor
     * @param removedLinks a list of links to be removed represented as pairs in the format {upstreamId, downstreamId}
     * @param addedLinks a list of links to be added represented as pairs in the format {upstreamId, downstreamId}
     * @param maxRetries the maximum amount of times this request should be retried in case of failure
     */
    TopologyChangeRequest(const std::vector<std::pair<WorkerId, WorkerId>>& removedLinks,
                          const std::vector<std::pair<WorkerId, WorkerId>>& addedLinks,
                          uint8_t maxRetries);

    /**
     * @brief Create a new topology change request
     * @param removedLinks a list of links to be removed represented as pairs in the format {upstreamId, downstreamId}
     * @param addedLinks a list of links to be added represented as pairs in the format {upstreamId, downstreamId}
     * @param maxRetries the maximum amount of times this request should be retried in case of failure
     * @return a pointer the the newly created request
     */
    static TopologyChangeRequestPtr create(const std::vector<std::pair<WorkerId, WorkerId>>& removedLinks,
                                           const std::vector<std::pair<WorkerId, WorkerId>>& addedLinks,
                                           uint8_t maxRetries);

    /**
     * @brief Executes the request logic.
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr>
    executeRequestLogic(const NES::RequestProcessor::StorageHandlerPtr& storageHandle) override;

    /**
     * @brief identify the operators affected by a topology change, run an incremental placement and deploy the changes
     * @param upstreamNodeId the id of the upstream node of removed link
     * @param downstreamNodeId the id of the downstream node of the removed link
     */
    void processRemoveTopologyLinkRequest(OperatorId upstreamNodeId, OperatorId downstreamNodeId);

    /**
     * @brief identify the operators of the specified shared query plan that are affected by a topology change,
     * run an incremental placement and deploy the changes
     * @param sharedQueryPlanId the id of the shared query plan for which the placemetn is to be updated
     * @param upstreamNodeId the id of the upstream node of removed link
     * @param downstreamNodeId the id of the downstream node of the removed link
     */
    void markOperatorsForReOperatorPlacement(SharedQueryId sharedQueryPlanId,
                                             const std::shared_ptr<ExecutionNode>& upstreamExecutionNode,
                                             const std::shared_ptr<ExecutionNode>& downstreamExecutionNode);

    /**
     * @brief iterate upstream from a given operator and find the first non system generated operator
     * @param downstreamOperator the operator to start the search at
     * @param downstreamWorkerId the id of the worker which the downstream operator is placed on
     * @param sharedQueryId the id of the shared query which the operator belongs to
     * @param globalExecutionPlan a pointer to the global execution plan
     * @return the closest upstream operator that is not system generated
     */
    static LogicalOperatorNodePtr findUpstreamNonSystemOperators(const LogicalOperatorNodePtr& downstreamOperator,
                                                                 WorkerId downstreamWorkerId,
                                                                 SharedQueryId sharedQueryId,
                                                                 const GlobalExecutionPlanPtr& globalExecutionPlan);

    /**
     * @brief itarate downstream fro ma given operator and find the first non system generated operator
     * @param upstreamOperator the operator to start the search at
     * @param upstreamWorkerId the id of the worker which the upstream operator is placed on
     * @param sharedQueryId the id of the shared query which the operator belongs to
     * @param globalExecutionPlan a pointer to the global execution plan
     * @return the closest downstream operator that is not system generated
     */
    static LogicalOperatorNodePtr findDownstreamNonSystemOperators(const LogicalOperatorNodePtr& upstreamOperator,
                                                                   WorkerId upstreamWorkerId,
                                                                   SharedQueryId sharedQueryId,
                                                                   const GlobalExecutionPlanPtr& globalExecutionPlan);

    /**
     * @brief identifies the upstream and downstream operators that can remain deployed on the same nodes as they were
     * before the topology change occurred. These sets can be used as input for an incremental placement. All operators
     * placed upstream of the set of upstream operators or downstream of the downstream operators can remain as is and
     * will not need to be re-placed
     * @param sharedQueryPlan the shared query id for which the up and downstream pinned operators need to be identified
     * @param upstreamNode the upstream node of the topology link that was removed
     * @param downstreamNode the downstream node of the topology link that was removed
     * @param topology a pointer to the topology
     * @param globalExecutionPlan a pointer to the global execution plan
     * @return a pair constaining two sets of operator id in the order {UPSTREAM, DOWNSTREAM}
     */
    static std::pair<std::set<OperatorId>, std::set<OperatorId>>
    findUpstreamAndDownstreamPinnedOperators(const SharedQueryPlanPtr& sharedQueryPlan,
                                 const ExecutionNodePtr& upstreamNode,
                                 const ExecutionNodePtr& downstreamNode,
                                 const TopologyPtr& topology,
                                 const GlobalExecutionPlanPtr& globalExecutionPlan);

    /**
     * @brief find all pairs of network sinks and sources that connect the specified up- and downstream node and belong
     * to the spcified shared query
     * @param sharedQueryPlanId the id of the shared query whose subplans are considered
     * @param upstreamNode the node hosting the network sinks
     * @param downstreamNode the node hosting the network sources
     * @return a vector source-sink-pairs in the format {SinkOperator, SourceOperator}
     */
    static std::vector<std::pair<LogicalOperatorNodePtr, LogicalOperatorNodePtr>>
    findNetworkOperatorsForLink(const SharedQueryId& sharedQueryPlanId,
                                const ExecutionNodePtr& upstreamNode,
                                const ExecutionNodePtr& downstreamNode);

    /**
     * @brief find the upstream network sink that sends data to the specified source
     * @param sharedQueryPlanId the id of the shared query plan to which source and sink belong
     * @param sourceWorkerId the id of the worker which hosts the source
     * @param networkSourceDescriptor the descriptor of the source whose upstream sink is wanted
     * @param globalExecutionPlan a pointer to the global execution plan
     * @return a pair containing the upstream sink operator and the id of the worker it is hosted on
     */
    static std::pair<SinkLogicalOperatorNodePtr, WorkerId>
    findUpstreamNetworkSinkAndWorkerId(const SharedQueryId& sharedQueryPlanId,
                                       WorkerId sourceWorkerId,
                                       const Network::NetworkSourceDescriptorPtr& networkSourceDescriptor,
                                       const GlobalExecutionPlanPtr& globalExecutionPlan);

    /**
     * @brief find the downstream network source that receives data from the specified sink
     * @param sharedQueryPlanId the id of the shared query plan to which source and sink belong
     * @param sinkWorkerId the id of the worker which hosts the source
     * @param networkSinkDescriptor the descriptor of the sink whose downstream source is wanted
     * @param globalExecutionPlan a pointer to the global execution plan
     * @return a pair containing the downstream source operator and the id of the worker it is hosted on
     */
    static std::pair<SourceLogicalOperatorNodePtr, WorkerId>
    findDownstreamNetworkSourceAndWorkerId(const SharedQueryId& sharedQueryPlanId,
                                           WorkerId sinkWorkerId,
                                           const Network::NetworkSinkDescriptorPtr& networkSinkDescriptor,
                                           const GlobalExecutionPlanPtr& globalExecutionPlan);

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution. std::exception_ptr is used to be able to allow setting an
     * exception state on the requests response promise without losing data to slicing in case the request cannot handle the
     * exception itself
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception thrown during request execution. std::exception_ptr is used to be able to allow setting an
     * exception state on the requests response promise without losing data to slicing in case the request cannot handle the
     * exception itself
     * @param storageHandle: The storage access handle used by the request
     */
    void preRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception thrown during request execution. std::exception_ptr is used to be able to allow setting an
     * exception state on the requests response promise without losing data to slicing in case the request cannot handle the
     * exception itself
     * @param storageHandle: The storage access handle used by the request
     */
    void postRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Performs steps to be done after execution of the request logic, e.g. unlocking the required data structures
     * @param storageHandle: The storage access handle used by the request
     */
    void postExecution(const StorageHandlerPtr& storageHandle) override;

  private:
    TopologyPtr topology;
    GlobalQueryPlanPtr globalQueryPlan;
    GlobalExecutionPlanPtr globalExecutionPlan;
    std::vector<std::pair<WorkerId, WorkerId>> removedLinks;
    std::vector<std::pair<WorkerId, WorkerId>> addedLinks;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
    QueryCatalogServicePtr queryCatalogService;
};
}// namespace RequestProcessor::Experimental
}// namespace NES
#endif//NES_TOPOLOGYCHANGEREQUEST_HPP
