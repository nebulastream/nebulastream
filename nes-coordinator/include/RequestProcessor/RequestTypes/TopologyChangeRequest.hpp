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

class TopologyChangeRequest : public AbstractUniRequest {
  public:
    TopologyChangeRequest(std::vector<std::pair<WorkerId, WorkerId>> removedLinks,
                          std::vector<std::pair<WorkerId, WorkerId>> addedLinks,
                          uint8_t maxRetries);

    static TopologyChangeRequestPtr create(std::vector<std::pair<WorkerId, WorkerId>> removedLinks,
                                           std::vector<std::pair<WorkerId, WorkerId>> addedLinks,
                                           uint8_t maxRetries);

    std::vector<AbstractRequestPtr>
    executeRequestLogic(const NES::RequestProcessor::StorageHandlerPtr& storageHandle) override;

    void processRemoveTopologyLinkRequest(unsigned long upstreamNodeId, unsigned long downstreamNodeId);

    void markOperatorsForReOperatorPlacement(unsigned long sharedQueryPlanId,
                                             const std::shared_ptr<ExecutionNode>& upstreamExecutionNode,
                                             const std::shared_ptr<ExecutionNode>& downstreamExecutionNode);

    static LogicalOperatorNodePtr findUpstreamNonSystemOperators(const LogicalOperatorNodePtr& downstreamOperator,
                                                                 WorkerId downstreamWorkerId,
                                                                 SharedQueryId sharedQueryId,
                                                                 const GlobalExecutionPlanPtr& globalExecutionPlan);
    static LogicalOperatorNodePtr findDownstreamNonSystemOperators(const LogicalOperatorNodePtr& upstreamOperator,
                                                                   WorkerId upstreamWorkerId,
                                                                   SharedQueryId sharedQueryId,
                                                                   const GlobalExecutionPlanPtr& globalExecutionPlan);
    static std::pair<std::set<OperatorId>, std::set<OperatorId>>
    findAffectedTopologySubGraph(const SharedQueryPlanPtr& sharedQueryPlan,
                                 const ExecutionNodePtr& upstreamNode,
                                 const ExecutionNodePtr& downstreamNode,
                                 const TopologyPtr& topology,
                                 const GlobalExecutionPlanPtr& globalExecutionPlan);

    static std::vector<std::pair<LogicalOperatorNodePtr, LogicalOperatorNodePtr>>
    findNetworkOperatorsForLink(const SharedQueryId& sharedQueryPlanId,
                                const ExecutionNodePtr& upstreamNode,
                                const ExecutionNodePtr& downstreamNode);

    static std::pair<SinkLogicalOperatorNodePtr, WorkerId>
    findUpstreamNetworkSinkAndWorkerId(const SharedQueryId& sharedQueryPlanId,
                                       const WorkerId workerId,
                                       const Network::NetworkSourceDescriptorPtr& networkSourceDescriptor,
                                       const GlobalExecutionPlanPtr& globalExecutionPlan);

    static std::pair<SourceLogicalOperatorNodePtr, WorkerId>
    findDownstreamNetworkSourceAndWorkerId(const SharedQueryId& sharedQueryPlanId,
                                           const WorkerId workerId,
                                           const Network::NetworkSinkDescriptorPtr& networkSinkDescriptor,
                                           const GlobalExecutionPlanPtr& globalExecutionPlan);
    void processRemoveTopologyNodeRequest(WorkerId removedNodeId);

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
