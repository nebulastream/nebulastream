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

#ifndef NES_Z3SIGNATUREBASEDPARTIALQUERYMERGERBOTTOMUPRULE_HPP
#define NES_Z3SIGNATUREBASEDPARTIALQUERYMERGERBOTTOMUPRULE_HPP

#include <Optimizer/QueryMerger/BaseQueryMergerRule.hpp>

namespace NES::Optimizer {

class SignatureEqualityUtil;
using SignatureEqualityUtilPtr = std::shared_ptr<SignatureEqualityUtil>;

class Z3SignatureBasedPartialQueryMergerBottomUpRule;
using Z3SignatureBasedPartialQueryMergerBottomUpRulePtr = std::shared_ptr<Z3SignatureBasedPartialQueryMergerBottomUpRule>;

/**
 * @brief SignatureBasedPartialQueryMergerRule is responsible for merging together all Queries sharing a common upstream operator
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
 * After running the SignatureBasedPartialQueryMergerRule, the resulting Global Query Plan will look as follow:
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
 */
class Z3SignatureBasedPartialQueryMergerBottomUpRule final : public BaseQueryMergerRule {

  public:
    static Z3SignatureBasedPartialQueryMergerBottomUpRulePtr create(z3::ContextPtr context);
    ~Z3SignatureBasedPartialQueryMergerBottomUpRule() noexcept final = default;

    bool apply(GlobalQueryPlanPtr globalQueryPlan) override;

  private:
    explicit Z3SignatureBasedPartialQueryMergerBottomUpRule(z3::ContextPtr context);
    /**
     * @brief identify if the query plans are equal or not
     * @param targetQueryPlan : target query plan
     * @param hostQueryPlan : host query plan
     * @return Map containing matching pair of target and host operators
     */
    std::map<LogicalOperatorNodePtr, LogicalOperatorNodePtr> areQueryPlansEqual(const QueryPlanPtr& targetQueryPlan,
                                                                                const QueryPlanPtr& hostQueryPlan);

    std::map<LogicalOperatorNodePtr, LogicalOperatorNodePtr> areOperatorEqual(const LogicalOperatorNodePtr& targetOperator,
                                                                              const LogicalOperatorNodePtr& hostOperator);
    SignatureEqualityUtilPtr signatureEqualityUtil;
};
}// namespace NES::Optimizer

#endif//NES_Z3SIGNATUREBASEDPARTIALQUERYMERGERBOTTOMUPRULE_HPP
