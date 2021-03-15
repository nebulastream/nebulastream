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

#ifndef NES_SIGNATUREBASEDPARTIALQUERYMERGERRULE_HPP
#define NES_SIGNATUREBASEDPARTIALQUERYMERGERRULE_HPP

#include <memory>

namespace NES {
class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;
}// namespace NES

namespace NES::Optimizer {

class SignatureBasedPartialQueryMergerRule;
typedef std::shared_ptr<SignatureBasedPartialQueryMergerRule> SignatureBasedPartialQueryMergerRulePtr;

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
 *                                                   GQN2({Map1},{Q1,Q2})
 *                                                           |
 *                                                  GQN3({Filter1},{Q1,Q2})
 *                                                           |
 *                                                GQN4({Source(Car)},{Q1,Q2})
 *
 */
class SignatureBasedPartialQueryMergerRule {

  public:
    static SignatureBasedPartialQueryMergerRulePtr create();
    ~SignatureBasedPartialQueryMergerRule();

    /**
     * @brief apply the rule on Global Query Plan
     * @param globalQueryPlan : the global query plan
     */
    bool apply(GlobalQueryPlanPtr globalQueryPlan);

  private:
    explicit SignatureBasedPartialQueryMergerRule();
};
}// namespace NES::Optimizer

#endif//NES_SIGNATUREBASEDPARTIALQUERYMERGERRULE_HPP
