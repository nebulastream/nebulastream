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

#ifndef NES_INCLUDE_OPTIMIZER_QUERYREWRITE_ORIGINIDINFERENCERULE_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYREWRITE_ORIGINIDINFERENCERULE_HPP_

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>

namespace NES::Optimizer {

class OriginIdInferenceRule;
using OriginIdInferenceRulePtr = std::shared_ptr<OriginIdInferenceRule>;

/**
 * @brief The OriginIdInferenceRule traverses the operator tree and assigns origin ids to operators.
 *
 * In general, origin ids are emitted from sources, windows or other stateful operators and are used to identify the origin of records.
 * This is crucial for stateful operators, which have to make guarantees over the order of records in the stream (see WatermarkProcessor)
 *
 * This rule is performed in two phases:
 * 1. It assigns unique origin ids to all operators, which inherit from OriginIdAssignmentOperator.
 * These origin ids are unique to a specific query.
 * 2. It processes all operators and assigns the input and output origin ids.
*/
class OriginIdInferenceRule : public BaseRewriteRule {
  public:
    static OriginIdInferenceRulePtr create();
    virtual ~OriginIdInferenceRule() = default;

    /**
     * @brief Apply the rule to the Query plan
     * @param queryPlanPtr : The original query plan
     * @return The updated query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

  private:
    explicit OriginIdInferenceRule();
};
}// namespace NES::Optimizer
#endif  // NES_INCLUDE_OPTIMIZER_QUERYREWRITE_ORIGINIDINFERENCERULE_HPP_
