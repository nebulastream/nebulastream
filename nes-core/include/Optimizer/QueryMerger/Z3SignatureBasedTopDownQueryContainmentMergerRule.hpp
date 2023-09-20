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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_QUERYMERGER_Z3SIGNATUREBASEDPARTIALQUERYCONTAINMENTMERGERRULE_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_QUERYMERGER_Z3SIGNATUREBASEDPARTIALQUERYCONTAINMENTMERGERRULE_HPP_

#include <Optimizer/QueryMerger/BaseQueryMergerRule.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentCheck.hpp>

namespace NES {
class SharedQueryPlan;
using SharedQueryPlanPtr = std::shared_ptr<SharedQueryPlan>;

class MatchedOperatorPair;
using MatchedOperatorPairPtr = std::shared_ptr<MatchedOperatorPair>;
}

namespace NES::Optimizer {

class SignatureContainmentCheck;
using SignatureContainmentUtilPtr = std::shared_ptr<SignatureContainmentCheck>;

class Z3SignatureBasedTopDownQueryContainmentMergerRule;
using Z3SignatureBasedPartialQueryContainmentMergerRulePtr = std::shared_ptr<Z3SignatureBasedTopDownQueryContainmentMergerRule>;

/**
 * @brief Z3SignatureBasedTopDownQueryContainmentMergerRule utilizes containment relationships for the merging process.
 * It identifies equivalent subqueries and passes on the correct operators for merging.
 * Further, it identifies containment relationships among subqueries and passes on the correct operators for merging.
 * After running this rule only a single representative operator chain should exists in the Global Query Plan for the common
 * upstream operator chain.
 * Effectively this rule will prune the global query plan to remove duplicate and contained operators.
 *
 * Example:
 * Given a Global Query Plan with two Global Query Node chains as follow:
 *                                                         GQPRoot
 *                                                         /     \
 *                                                       /        \
 *                                                     /           \
 *                                         GQN1({Sink1},{Q1})  GQN5({Sink2},{Q2})
 *                                                |                 |
 *                                        GQN2({Map1},{Q1})    GQN7({Filter1},{Q2})
 *                                                |                 |
 *                                     GQN3({Filter2},{Q1})    GQN6({Map1},{Q2})
 *                                                |                 |
 *                                  GQN4({Source(Car)},{Q1})   GQN8({Source(Car)},{Q2})
 *
 *
 * where Filter1 contains Filter2.
 * After running the Z3SignatureBasedPartialQueryMergerRule, the resulting Global Query Plan will look as follow:
 *
 *                                                         GQPRoot
 *                                                         /     \
 *                                                        /       \
 *                                                       |    GQN1({Sink1},{Q1})
 *                                                       |         |
 *                                           GQN1({Sink1},{Q1}) GQN3({Filter2},{Q1})
 *                                                       |         |
 *                                                 GQN7({Filter1},{Q2})
 *                                                           |
 *                                                  GQN6({Map1},{Q2})
 *                                                           |
 *                                                GQN4({Source(Car)},{Q1,Q2})
 *
 * Additionally information: In case a containment relationship was detected by the signature containment util, the contained operations from the
 * contained query will be added to the equivalent operator chain of the container query. We can do this for
 * 1. filter operations: All upstream filter predicates from the contained query will be extracted and combined to one filter predicate. This filter
 * will then be added as parent to the container operator together with the remaining contained operator chain
 * This does not work, in case a map transformation is assigned to a filter predicate and we would need to pull up the filter from below that map operator.
 * 2. projection operations: We extract all upstream projection operators and add the most downstream projection operator to the
 * container's upstream operator chain
 * 3. window operations: We extract all upstream window operators, identify the contained window operator, and add it to the container's
 * upstream operator chain
 */
class Z3SignatureBasedTopDownQueryContainmentMergerRule final : public BaseQueryMergerRule {

  public:
    static Z3SignatureBasedPartialQueryContainmentMergerRulePtr create(z3::ContextPtr context, bool allowSQPAsContainee);
    ~Z3SignatureBasedTopDownQueryContainmentMergerRule() noexcept final = default;

    bool apply(GlobalQueryPlanPtr globalQueryPlan) override;

  private:
    explicit Z3SignatureBasedTopDownQueryContainmentMergerRule(z3::ContextPtr context, bool allowSQPAsContainee);

    SignatureContainmentUtilPtr SignatureContainmentUtil;

};
}// namespace NES::Optimizer

#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYMERGER_Z3SIGNATUREBASEDPARTIALQUERYMERGERRULE_HPP_
