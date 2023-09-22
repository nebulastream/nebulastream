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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_PHASES_QUERYMERGERPHASE_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_PHASES_QUERYMERGERPHASE_HPP_

#include <Optimizer/QueryMerger/BaseQueryMergerRule.hpp>
#include <iostream>
#include <map>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES::Configurations {
class OptimizerConfiguration;
using OptimizerConfigurationPtr = std::shared_ptr<OptimizerConfiguration>;
}// namespace Configurations

namespace NES::Optimizer {

enum class QueryMergerRule : uint8_t {
    SyntaxBasedCompleteQueryMergerRule,
    SyntaxBasedPartialQueryMergerRule,
    Z3SignatureBasedCompleteQueryMergerRule,
    Z3SignatureBasedPartialQueryMergerRule,
    Z3SignatureBasedPartialQueryMergerBottomUpRule,
    HashSignatureBasedCompleteQueryMergerRule,
    ImprovedHashSignatureBasedCompleteQueryMergerRule,
    ImprovedHashSignatureBasedPartialQueryMergerRule,
    HashSignatureBasedPartialQueryMergerRule,
    DefaultQueryMergerRule,
    HybridCompleteQueryMergerRule,
    Z3SignatureBasedBottomUpQueryContainmentRule,
    Z3SignatureBasedTopDownQueryContainmentMergerRule
};

class QueryMergerPhase;
using QueryMergerPhasePtr = std::shared_ptr<QueryMergerPhase>;

class QueryMergerPhase {

  public:
    static QueryMergerPhasePtr create(z3::ContextPtr context, const Configurations::OptimizerConfiguration optimizerConfiguration);

    /**
     * @brief execute method to apply different query merger rules on the global query plan.
     * @param globalQueryPlan: the global query plan
     * @return true if successful
     */
    bool execute(GlobalQueryPlanPtr globalQueryPlan);

  private:
    explicit QueryMergerPhase(z3::ContextPtr context, const Configurations::OptimizerConfiguration optimizerConfiguration);
    BaseQueryMergerRulePtr queryMergerRule;
};
}// namespace NES::Optimizer
#endif// NES_CORE_INCLUDE_OPTIMIZER_PHASES_QUERYMERGERPHASE_HPP_
