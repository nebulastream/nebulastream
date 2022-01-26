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

#ifndef NES_INCLUDE_PLANS_UTILS_PLANJSONGENERATOR_HPP_
#define NES_INCLUDE_PLANS_UTILS_PLANJSONGENERATOR_HPP_

#include <Plans/Query/QueryId.hpp>

namespace web {
namespace json {
class value;
}// namespace json
}// namespace web

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

/**
 * @brief This is a utility class to convert different plans into JSON
 */
class PlanJsonGenerator {

  public:
    /**
     * @brief function to generate the query plan from of a query.
     * @param queryCatalog in which the query is stored
     * @param the id of the query
     * @return a JSON object representing the query plan
     */
    static web::json::value getQueryPlanAsJson(const QueryPlanPtr& queryPlan);

    /**
     * @brief get the json representation of execution plan of a query
     * @param the global execution plan
     * @param id of the query
     * @return
     */
    static web::json::value getExecutionPlanAsJson(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                   QueryId queryId = INVALID_QUERY_ID);

  private:
    /**
     * @brief function to traverse to queryPlanChildren
     * @param root root operator of the queryPlan
     * @param nodes JSON array to store the traversed node
     * @param edges JSON array to store the traversed edge
     */
    static void
    getChildren(OperatorNodePtr const& root, std::vector<web::json::value>& nodes, std::vector<web::json::value>& edges);

    /**
     * @param an operator node
     * @return the type of operator in String
     */
    static std::string getOperatorType(const OperatorNodePtr& operatorNode);
};
}// namespace NES
#endif  // NES_INCLUDE_PLANS_UTILS_PLANJSONGENERATOR_HPP_
