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

#ifndef NES_COORDINATOR_INCLUDE_PHASES_GLOBALQUERYPLANUPDATEPHASE_HPP_
#define NES_COORDINATOR_INCLUDE_PHASES_GLOBALQUERYPLANUPDATEPHASE_HPP_

#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <memory>
#include <vector>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {

namespace Experimental {
class RemoveTopologyNodeRequest;
using RemoveTopologyNodeRequestPtr = std::shared_ptr<RemoveTopologyNodeRequest>;

class RemoveTopologyLinkRequest;
using RemoveTopologyLinkRequestPtr = std::shared_ptr<RemoveTopologyLinkRequest>;
}// namespace Experimental

class AddQueryRequest;
using AddQueryRequestPtr = std::shared_ptr<AddQueryRequest>;

class StopQueryRequest;
using StopQueryRequestPtr = std::shared_ptr<StopQueryRequest>;

class FailQueryRequest;
using FailQueryRequestPtr = std::shared_ptr<FailQueryRequest>;

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

namespace Optimizer {
class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

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
     * @param topology : the topology
     * @param queryCatalogService : the query catalog service
     * @param sourceCatalog : the source catalog
     * @param globalQueryPlan : the empty global query plan
     * @param z3Context : the z3 context
     * @param coordinatorConfiguration configuration of the coordinator
     * @param udfCatalog : the udf catalog
     * @param globalExecutionPlan : the global query plan
     * @return the shared pointer of the global query plan update phase
     */
    static GlobalQueryPlanUpdatePhasePtr create(const TopologyPtr& topology,
                                                const QueryCatalogServicePtr& queryCatalogService,
                                                const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                const GlobalQueryPlanPtr& globalQueryPlan,
                                                const z3::ContextPtr& z3Context,
                                                const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration,
                                                const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
                                                const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan);

    /**
     * @brief This method executes the Global Query Plan Update Phase on a batch of query requests
     * @param queryRequests: a batch of query requests (in the form of Query Catalog Entry) to be processed to update global query plan
     * @return Shared pointer to the Global Query Plan for further processing
     */
    GlobalQueryPlanPtr execute(const std::vector<NESRequestPtr>& nesRequests);

  private:
    explicit GlobalQueryPlanUpdatePhase(const TopologyPtr& topology,
                                        const QueryCatalogServicePtr& queryCatalogService,
                                        const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                        const GlobalQueryPlanPtr& globalQueryPlan,
                                        const z3::ContextPtr& z3Context,
                                        const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration,
                                        const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
                                        const GlobalExecutionPlanPtr& globalExecutionPlan);

    /**
     * @brief Process add query request
     * @param addQueryRequest:add query request
     */
    void processAddQueryRequest(const AddQueryRequestPtr& addQueryRequest);

    /**
     * @brief Process fail query request
     * @param failQueryRequest: fail query request
     */
    void processFailQueryRequest(const FailQueryRequestPtr& failQueryRequest);

    /**
     * @brief Process stop query request
     * @param stopQueryRequest: stop query request
     */
    void processStopQueryRequest(const StopQueryRequestPtr& stopQueryRequest);

    TopologyPtr topology;
    GlobalExecutionPlanPtr globalExecutionPlan;
    QueryCatalogServicePtr queryCatalogService;
    GlobalQueryPlanPtr globalQueryPlan;
    TypeInferencePhasePtr typeInferencePhase;
    QueryRewritePhasePtr queryRewritePhase;
    TopologySpecificQueryRewritePhasePtr topologySpecificQueryRewritePhase;
    QueryMergerPhasePtr queryMergerPhase;
    SignatureInferencePhasePtr signatureInferencePhase;
    OriginIdInferencePhasePtr originIdInferencePhase;
    MemoryLayoutSelectionPhasePtr setMemoryLayoutPhase;
    SampleCodeGenerationPhasePtr sampleCodeGenerationPhase;
    z3::ContextPtr z3Context;
};
}// namespace Optimizer
}// namespace NES
#endif// NES_COORDINATOR_INCLUDE_PHASES_GLOBALQUERYPLANUPDATEPHASE_HPP_
