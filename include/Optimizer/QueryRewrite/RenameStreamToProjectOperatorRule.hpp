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

#ifndef NES_RENAMESTREAMTOPROJECTOPERATORRULE_HPP
#define NES_RENAMESTREAMTOPROJECTOPERATORRULE_HPP

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES {

class RenameStreamToProjectOperatorRule;
typedef std::shared_ptr<RenameStreamToProjectOperatorRule> RenameStreamToProjectOperatorRulePtr;

/**
 * @brief This rule is responsible for transforming Stream Rename operator to the projection operator
 */
class RenameStreamToProjectOperatorRule : public BaseRewriteRule {

  public:
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

    static RenameStreamToProjectOperatorRulePtr create();

  private:
    RenameStreamToProjectOperatorRule() = default;

    /**
     * @brief Convert input operator into project operator
     * @param operatorNode : the rename stream operator
     * @return pointer to the converted project operator
     */
    OperatorNodePtr convert(OperatorNodePtr operatorNode);
};

}// namespace NES

#endif//NES_RENAMESTREAMTOPROJECTOPERATORRULE_HPP
