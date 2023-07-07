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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_PHASES_GLOBALQUERYPLANUPDATEPHASE_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_PHASES_GLOBALQUERYPLANUPDATEPHASE_HPP_

#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <memory>
#include <vector>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {

namespace Configurations {
class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;
}// namespace Configurations

namespace Catalogs {

namespace Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Source

namespace UDF {
class UDFCatalog;
using UDFCatalogPtr = std::shared_ptr<UDFCatalog>;
}// namespace UDF

}// namespace Catalogs

class Request;
using NESRequestPtr = std::shared_ptr<Request>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

namespace Optimizer {

class GlobalQueryPlanUpdatePhase;
using GlobalQueryPlanUpdatePhasePtr = std::shared_ptr<GlobalQueryPlanUpdatePhase>;

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class QueryRewritePhase;
using QueryRewritePhasePtr = std::shared_ptr<QueryRewritePhase>;

class SampleCodeGenerationPhase;
using SampleCodeGenerationPhasePtr = std::shared_ptr<SampleCodeGenerationPhase>;

class OriginIdInferencePhase;
using OriginIdInferencePhasePtr = std::shared_ptr<OriginIdInferencePhase>;

class TopologySpecificQueryRewritePhase;
using TopologySpecificQueryRewritePhasePtr = std::shared_ptr<TopologySpecificQueryRewritePhase>;

class SignatureInferencePhase;
using SignatureInferencePhasePtr = std::shared_ptr<SignatureInferencePhase>;

class QueryMergerPhase;
using QueryMergerPhasePtr = std::shared_ptr<QueryMergerPhase>;

class MemoryLayoutSelectionPhase;
using MemoryLayoutSelectionPhasePtr = std::shared_ptr<MemoryLayoutSelectionPhase>;

/**
 * @brief This class is responsible for accepting a batch of query requests and then updating the Global Query Plan accordingly.
 */
class GlobalQueryPlanUpdatePhase {
  public:
    /**
     * @brief Create an instance of the GlobalQueryPlanUpdatePhase
     * @param queryCatalogService: the catalog of queryIdAndCatalogEntryMapping
     * @param sourceCatalog: the catalog of sources
     * @param globalQueryPlan: the input global query plan
     * @param coordinatorConfiguration: configuration of the coordinator
     * @return Shared pointer for the GlobalQueryPlanUpdatePhase
     */
    static GlobalQueryPlanUpdatePhasePtr create(TopologyPtr topology,
                                                QueryCatalogServicePtr queryCatalogService,
                                                Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                                GlobalQueryPlanPtr globalQueryPlan,
                                                z3::ContextPtr z3Context,
                                                const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration,
                                                Catalogs::UDF::UDFCatalogPtr udfCatalog,
                                                GlobalExecutionPlanPtr globalExecutionPlan);

    /**
     * @brief This method executes the Global Query Plan Update Phase on a batch of query requests
     * @param queryRequests: a batch of query requests (in the form of Query Catalog Entry) to be processed to update global query plan
     * @return Shared pointer to the Global Query Plan for further processing
     */
    GlobalQueryPlanPtr execute(const std::vector<NESRequestPtr>& nesRequests);

  private:
    explicit GlobalQueryPlanUpdatePhase(TopologyPtr topology,
                                        QueryCatalogServicePtr queryCatalogService,
                                        Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                        GlobalQueryPlanPtr globalQueryPlan,
                                        z3::ContextPtr z3Context,
                                        const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration,
                                        Catalogs::UDF::UDFCatalogPtr udfCatalog,
                                        GlobalExecutionPlanPtr globalExecutionPlan);

    /**
     * @brief Process add query request
     * @param nesRequest:add query request
     */
    void processAddQueryRequest(const NESRequestPtr& nesRequest);

    /**
     * @brief Process fail query request
     * @param nesRequest: fail query request
     */
    void processFailQueryRequest(const NESRequestPtr& nesRequest);

    /**
     * @brief Process stop query request
     * @param nesRequest: stop query request
     */
    void processStopQueryRequest(const NESRequestPtr& nesRequest);

    /**
     * @brief Process Remove Topology Link request
     * @param nesRequest: Remove Topology Link request
     */
    void processRemoveTopologyLinkRequest(const NESRequestPtr& nesRequest);

    /**
     * @brief Process Remove Topology Node request
     * @param nesRequest: Remove Topology Node request
     */
    void processRemoveTopologyNodeRequest(const NESRequestPtr& nesRequest);

    /**
     * @brief Mark operators of shared query plans that are placed between upstream and downstream execution nodes for re-operator placement.
     * Note: If the upstream or downstream execution node consists only of system generated operators then successive execution
     * nodes are explored till a logical operator is found.
     * @param sharedQueryId: id of the shared query plan
     * @param upstreamExecutionNode: the upstream execution node
     * @param downstreamExecutionNode: the downstream execution node
     */
    void markOperatorsForReOperatorPlacement(SharedQueryId sharedQueryId,
                                             const ExecutionNodePtr& upstreamExecutionNode,
                                             const ExecutionNodePtr& downstreamExecutionNode);

    TopologyPtr topology;
    GlobalExecutionPlanPtr globalExecutionPlan;
    QueryCatalogServicePtr queryCatalogService;
    GlobalQueryPlanPtr globalQueryPlan;
    TypeInferencePhasePtr typeInferencePhase;
    QueryRewritePhasePtr queryRewritePhase;
    TopologySpecificQueryRewritePhasePtr topologySpecificQueryRewritePhase;
    Optimizer::QueryMergerPhasePtr queryMergerPhase;
    Optimizer::SignatureInferencePhasePtr signatureInferencePhase;
    OriginIdInferencePhasePtr originIdInferencePhase;
    MemoryLayoutSelectionPhasePtr setMemoryLayoutPhase;
    SampleCodeGenerationPhasePtr sampleCodeGenerationPhase;
    z3::ContextPtr z3Context;
    void getDownstreamPinnedOperatorIds(SharedQueryId sharedQueryPlanId,
                                         const ExecutionNodePtr& downstreamExecutionNode,
                                         std::set<OperatorId>& downstreamOperatorIds) const;
    void getUpstreamPinnedOperatorIds(SharedQueryId sharedQueryPlanId,
                                      const ExecutionNodePtr& upstreamExecutionNode,
                                      std::set<OperatorId>& upstreamOperatorIds) const;
};
}// namespace Optimizer
}// namespace NES
#endif// NES_CORE_INCLUDE_OPTIMIZER_PHASES_GLOBALQUERYPLANUPDATEPHASE_HPP_
