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

#ifndef NES_INCLUDE_OPTIMIZER_QUERY_REWRITE_RENAME_STREAM_TO_PROJECT_OPERATOR_RULE_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERY_REWRITE_RENAME_STREAM_TO_PROJECT_OPERATOR_RULE_HPP_

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES::Optimizer {

class RenameStreamToProjectOperatorRule;
using RenameStreamToProjectOperatorRulePtr = std::shared_ptr<RenameStreamToProjectOperatorRule>;

/**
 * @brief This rule is responsible for transforming Stream Rename operator to the projection operator
 */
class RenameStreamToProjectOperatorRule : public BaseRewriteRule {

  public:
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;
    virtual ~RenameStreamToProjectOperatorRule() = default;

    static RenameStreamToProjectOperatorRulePtr create();

  private:
    RenameStreamToProjectOperatorRule() = default;

    /**
     * @brief Convert input operator into project operator
     * @param operatorNode : the rename stream operator
     * @return pointer to the converted project operator
     */
    static OperatorNodePtr convert(const OperatorNodePtr& operatorNode);
};

}// namespace NES::Optimizer

#endif// NES_INCLUDE_OPTIMIZER_QUERY_REWRITE_RENAME_STREAM_TO_PROJECT_OPERATOR_RULE_HPP_
