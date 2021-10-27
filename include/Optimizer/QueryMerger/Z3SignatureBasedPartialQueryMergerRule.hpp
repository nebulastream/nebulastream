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

#ifndef NES_INCLUDE_OPTIMIZER_QUERY_MERGER_Z3_SIGNATURE_BASED_PARTIAL_QUERY_MERGER_RULE_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERY_MERGER_Z3_SIGNATURE_BASED_PARTIAL_QUERY_MERGER_RULE_HPP_

#include <Optimizer/QueryMerger/BaseQueryMergerRule.hpp>

namespace NES::Optimizer {

class SignatureEqualityUtil;
using SignatureEqualityUtilPtr = std::shared_ptr<SignatureEqualityUtil>;

class Z3SignatureBasedPartialQueryMergerRule;
using Z3SignatureBasedPartialQueryMergerRulePtr = std::shared_ptr<Z3SignatureBasedPartialQueryMergerRule>;

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
class Z3SignatureBasedPartialQueryMergerRule final : public BaseQueryMergerRule {

  public:
    static Z3SignatureBasedPartialQueryMergerRulePtr create(z3::ContextPtr context);
    ~Z3SignatureBasedPartialQueryMergerRule() noexcept final = default;

    bool apply(GlobalQueryPlanPtr globalQueryPlan) override;

  private:
    explicit Z3SignatureBasedPartialQueryMergerRule(z3::ContextPtr context);
    SignatureEqualityUtilPtr signatureEqualityUtil;
};
}// namespace NES::Optimizer

#endif  // NES_INCLUDE_OPTIMIZER_QUERY_MERGER_Z3_SIGNATURE_BASED_PARTIAL_QUERY_MERGER_RULE_HPP_
