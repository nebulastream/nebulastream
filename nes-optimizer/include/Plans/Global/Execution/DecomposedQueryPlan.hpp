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

#ifndef NES_QUERYSUBPLAN_HPP
#define NES_QUERYSUBPLAN_HPP

#include <Identifiers.hpp>
#include <Util/QueryState.hpp>
#include <memory>
#include <vector>

namespace NES {

class LogicalOperatorNode;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperatorNode>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

/**
 * @brief This represents the plan that need to be executed on the worker node
 */
class DecomposedQueryPlan {

  public:
    static DecomposedQueryPlanPtr create(DecomposedQueryPlanId decomposedQueryPlanId, SharedQueryId sharedQueryId);

    explicit DecomposedQueryPlan(DecomposedQueryPlanId decomposedQueryPlanId, SharedQueryId sharedQueryId);

    /**
     * @brief Get state of the query plan
     * @return query state
     */
    QueryState getQueryState();

    /**
     * @brief Set state of the query plan
     * @param queryState : new query plan state
     */
    void setQueryState(QueryState queryState);

    /**
     * @brief Get version of the query plan
     * @return current version
     */
    QuerySubPlanVersion getVersion();

    /**
     * @brief Set new version for the query sub plan
     * @param newVersion: new version of the query sub plan
     */
    void setVersion(QuerySubPlanVersion newVersion);

    /**
     * @brief Create copy of the decomposed query plan
     * @return copy
     */
    DecomposedQueryPlanPtr copy();

  private:
    DecomposedQueryPlanId decomposedQueryPlanId;
    SharedQueryId sharedQueryId;
    QueryState queryState;
    std::vector<LogicalOperatorNodePtr> rootOperators;
    uint32_t version;
};

}// namespace NES

#endif//NES_QUERYSUBPLAN_HPP
