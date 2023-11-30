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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_MAPUDFSTOOPENCLOPERATORSRULE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_MAPUDFSTOOPENCLOPERATORSRULE_HPP_

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES::Optimizer {

class MapUDFsToOpenCLOperatorsRule;
using MapUDFsToOpenCLOperatorsRulePtr = std::shared_ptr<MapUDFsToOpenCLOperatorsRule>;

/**
 * @brief: This rule converts a Map Java UDF into OpenCL operator such that it can be accelerated over CPU or GPU.
 * This rule is activated using elegant configuration elegant.accelerateJavaUDFs = true.
 */
class MapUDFsToOpenCLOperatorsRule : public BaseRewriteRule {

  public:
    static MapUDFsToOpenCLOperatorsRulePtr create();

    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

    virtual ~MapUDFsToOpenCLOperatorsRule() = default;

  private:
    MapUDFsToOpenCLOperatorsRule() = default;
};

}// namespace NES::Optimizer
#endif  // NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_MAPUDFSTOOPENCLOPERATORSRULE_HPP_
