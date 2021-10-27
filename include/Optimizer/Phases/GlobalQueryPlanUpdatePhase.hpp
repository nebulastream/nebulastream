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

#ifndef NES_INCLUDE_OPTIMIZER_PHASES_GLOBAL_QUERY_PLAN_UPDATE_PHASE_HPP_
#define NES_INCLUDE_OPTIMIZER_PHASES_GLOBAL_QUERY_PLAN_UPDATE_PHASE_HPP_

#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <memory>
#include <vector>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {

class NESRequest;
using NESRequestPtr = std::shared_ptr<NESRequest>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;

class StreamCatalog;
using StreamCatalogPtr = std::shared_ptr<StreamCatalog>;

}// namespace NES

namespace NES::Optimizer {

class GlobalQueryPlanUpdatePhase;
using GlobalQueryPlanUpdatePhasePtr = std::shared_ptr<GlobalQueryPlanUpdatePhase>;

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class QueryRewritePhase;
using QueryRewritePhasePtr = std::shared_ptr<QueryRewritePhase>;

class TopologySpecificQueryRewritePhase;
using TopologySpecificQueryRewritePhasePtr = std::shared_ptr<TopologySpecificQueryRewritePhase>;

class SignatureInferencePhase;
using SignatureInferencePhasePtr = std::shared_ptr<SignatureInferencePhase>;

class QueryMergerPhase;
using QueryMergerPhasePtr = std::shared_ptr<QueryMergerPhase>;

/**
 * @brief This class is responsible for accepting a batch of query requests and then updating the Global Query Plan accordingly.
 */
class GlobalQueryPlanUpdatePhase {
  public:
    /**
     * @brief Create an instance of the GlobalQueryPlanUpdatePhase
     * @param queryCatalog: the catalog of queries
     * @param streamCatalog: the catalog of streams
     * @param globalQueryPlan: the input global query plan
     * @param enableQueryMerging: enable or disable query merging
     * @param queryMergerRule: Rule to be used fro performing query merging if merging enabled
     * @return Shared pointer for the GlobalQueryPlanUpdatePhase
     */
    static GlobalQueryPlanUpdatePhasePtr create(QueryCatalogPtr queryCatalog,
                                                StreamCatalogPtr streamCatalog,
                                                GlobalQueryPlanPtr globalQueryPlan,
                                                z3::ContextPtr z3Context,
                                                Optimizer::QueryMergerRule queryMergerRule);

    /**
     * @brief This method executes the Global Query Plan Update Phase on a batch of query requests
     * @param queryRequests: a batch of query requests (in the form of Query Catalog Entry) to be processed to update global query plan
     * @return Shared pointer to the Global Query Plan for further processing
     */
    GlobalQueryPlanPtr execute(const std::vector<NESRequestPtr>& nesRequests);

  private:
    explicit GlobalQueryPlanUpdatePhase(QueryCatalogPtr queryCatalog,
                                        const StreamCatalogPtr& streamCatalog,
                                        GlobalQueryPlanPtr globalQueryPlan,
                                        z3::ContextPtr z3Context,
                                        Optimizer::QueryMergerRule queryMergerRule);

    QueryCatalogPtr queryCatalog;
    GlobalQueryPlanPtr globalQueryPlan;
    TypeInferencePhasePtr typeInferencePhase;
    QueryRewritePhasePtr queryRewritePhase;
    TopologySpecificQueryRewritePhasePtr topologySpecificQueryRewritePhase;
    Optimizer::QueryMergerPhasePtr queryMergerPhase;
    Optimizer::SignatureInferencePhasePtr signatureInferencePhase;
    z3::ContextPtr z3Context;
};
}// namespace NES::Optimizer

#endif  // NES_INCLUDE_OPTIMIZER_PHASES_GLOBAL_QUERY_PLAN_UPDATE_PHASE_HPP_
