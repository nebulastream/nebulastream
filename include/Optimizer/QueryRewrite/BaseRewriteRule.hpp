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

#ifndef NES_IMPL_OPTIMIZER_QUERYREWRITE_BASERULE_HPP_
#define NES_IMPL_OPTIMIZER_QUERYREWRITE_BASERULE_HPP_

#include <Util/Logger.hpp>
#include <memory>

namespace NES {

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class BaseRefinementRule : public std::enable_shared_from_this<BaseRefinementRule> {

  public:
    /**
     * @brief Apply the rule to the Query plan
     * @param queryPlanPtr : The original query plan
     * @return The updated query plan
     */
    virtual QueryPlanPtr apply(QueryPlanPtr queryPlanPtr) = 0;

    /**
     * @brief Checks if the current node is of type RuleType
     * @tparam RefinementType
     * @return bool true if node is of RuleType
     */
    template<class RefinementType>
    const bool instanceOf() {
        if (dynamic_cast<RefinementType*>(this)) {
            return true;
        };
        return false;
    };

    /**
    * @brief Dynamically casts the node to a RuleType
    * @tparam RefinementType
    * @return returns a shared pointer of the RuleType
    */
    template<class RefinementType>
    std::shared_ptr<RefinementType> as() {
        if (instanceOf<RefinementType>()) {
            return std::dynamic_pointer_cast<RefinementType>(this->shared_from_this());
        } else {
            NES_FATAL_ERROR("We performed an invalid cast");
            throw std::bad_cast();
        }
    }
};
}// namespace NES

#endif//NES_IMPL_OPTIMIZER_QUERYREWRITE_BASERULE_HPP_
