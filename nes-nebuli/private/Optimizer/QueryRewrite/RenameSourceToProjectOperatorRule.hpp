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

#pragma once

#include <memory>
#include <Operators/Operator.hpp>
#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES::Optimizer
{


/**
 * @brief This rule is responsible for transforming Source Rename operator to the projection operator
 */
class RenameSourceToProjectOperatorRule : public BaseRewriteRule
{
public:
    std::shared_ptr<QueryPlan> apply(std::shared_ptr<QueryPlan> queryPlan) override;
    virtual ~RenameSourceToProjectOperatorRule() = default;

    static std::shared_ptr<RenameSourceToProjectOperatorRule> create();

private:
    RenameSourceToProjectOperatorRule() = default;

    /**
     * @brief Convert input operator into project operator
     * @param operatorNode : the rename source operator
     * @return pointer to the converted project operator
     */
    static std::shared_ptr<Operator> convert(const std::shared_ptr<Operator>& operatorNode);
};

}
