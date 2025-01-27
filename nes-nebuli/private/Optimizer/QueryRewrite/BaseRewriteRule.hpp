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

namespace NES
{
class QueryPlan;
}

namespace NES::Optimizer
{

class BaseRewriteRule : public std::enable_shared_from_this<BaseRewriteRule>
{
public:
    /**
     * @brief Apply the rule to the Query plan
     * @param queryPlan: The original query plan
     * @return The updated query plan
     */
    virtual std::shared_ptr<QueryPlan> apply(std::shared_ptr<QueryPlan> queryPlan) = 0;

    /**
     * @brief Checks if the current node is of type RuleType
     * @tparam RefinementType
     * @return bool true if node is of RuleType
     */
    template <class RefinementType>
    bool instanceOf()
    {
        if (dynamic_cast<RefinementType*>(this))
        {
            return true;
        };
        return false;
    };
};
}
