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
#include <Optimizer/QuerySignatures/SignatureContainmentUtil.hpp>

namespace NES {
class SharedQueryPlan;
using SharedQueryPlanPtr = std::shared_ptr<SharedQueryPlan>;
}

namespace NES::Optimizer {

class SignatureContainmentUtil;
using SignatureContainmentUtilPtr = std::shared_ptr<SignatureContainmentUtil>;

class Z3SignatureBasedPartialQueryContainmentMergerRule;
using Z3SignatureBasedPartialQueryContainmentMergerRulePtr = std::shared_ptr<Z3SignatureBasedPartialQueryContainmentMergerRule>;

/**
 * @brief Z3SignatureBasedPartialQueryContainmentMergerRule is responsible for merging together all Queries sharing a common upstream operator
 * chain. After running this rule only a single representative operator chain should exists in the Global Query Plan for the common
 * upstream operator chain.
 * Effectively this rule will prune the global query plan for duplicate operators.
 *
 * Following is the example:
 * Given a Global Query Plan with two Global Query Node chains as follow:
 *                                                         GQPRoot
 *                                                         /     \
 *                                                       /        \
 *                                                     /           \
 *                                         GQN1({Sink1},{Q1})  GQN5({Sink2},{Q2})
 *                                                |                 |
 *                                        GQN2({Map2},{Q1})    GQN6({Map1},{Q2})
 *                                                |                 |
 *                                     GQN3({Filter1},{Q1})    GQN7({Filter1},{Q2})
 *                                                |                 |
 *                                  GQN4({Source(Car)},{Q1})   GQN8({Source(Car)},{Q2})
 *
 *
 * After running the Z3SignatureBasedPartialQueryMergerRule, the resulting Global Query Plan will look as follow:
 *
 *                                                         GQPRoot
 *                                                         /     \
 *                                                        /       \
 *                                           GQN1({Sink1},{Q1}) GQN5({Sink2},{Q2})
 *                                                       |         |
 *                                           GQN2({Map2},{Q1}) GQN6({Map1},{Q2})
 *                                                        \      /
 *                                                         \   /
 *                                                  GQN3({Filter1},{Q1,Q2})
 *                                                           |
 *                                                GQN4({Source(Car)},{Q1,Q2})
 *
 * Additionally, in case a containment relationship was detected by the signature containment util, the contained operations from the
 * contained query will be added to the equivalent operator chain of the container query. We can do this for
 * 1. filter operations: All upstream filter operators from the contained query will be extracted and added to the equivalent
 * container's upstream operator chain
 * 2. projection operations: We extract all upstream projection operators and add the most downstream projection operator to the
 * container's upstream operator chain
 * 3. window operations: We extract all upstream window operators, identify the contained window operator, and add it to the container's
 * upstream operator chain
 */
class Z3SignatureBasedPartialQueryContainmentMergerRule final : public BaseQueryMergerRule {

  public:
    static Z3SignatureBasedPartialQueryContainmentMergerRulePtr create(z3::ContextPtr context);
    ~Z3SignatureBasedPartialQueryContainmentMergerRule() noexcept final = default;

    bool apply(GlobalQueryPlanPtr globalQueryPlan) override;

  private:
    explicit Z3SignatureBasedPartialQueryContainmentMergerRule(z3::ContextPtr context);

    /**
     * @brief adds the containment operator chain to the correct container operator
     * * 1. filter operations: All upstream filter operators from the contained query will be extracted and added to the equivalent
     * container's upstream operator chain
     * 2. projection operations: We extract all upstream projection operators and add the most downstream projection operator to the
     * container's upstream operator chain
     * 3. window operations: We extract all upstream window operators, identify the contained window operator, and add it to the container's
     * upstream operator chain
     * @param containerQueryPlan the containers query plan to add the contained operator chain to
     * @param containerOperator the current container operator
     * @param containedOperatorChain vector with all extracted operators from the contained query
     */
    void addContainmentOperatorChain(
        SharedQueryPlanPtr& containerQueryPlan,
        const OperatorNodePtr& containerOperator,
        const OperatorNodePtr& containedOperator,
        const std::vector<LogicalOperatorNodePtr> containedOperatorChain) const;

    SignatureContainmentUtilPtr SignatureContainmentUtil;

};
}// namespace NES::Optimizer

#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYMERGER_Z3SIGNATUREBASEDPARTIALQUERYMERGERRULE_HPP_
