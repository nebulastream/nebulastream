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

#ifndef NES_PLANJSONGENERATOR_HPP
#define NES_PLANJSONGENERATOR_HPP

#include <Plans/Query/QueryId.hpp>
#include <cpprest/json.h>

namespace NES {

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

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
    static web::json::value getQueryPlanAsJson(QueryPlanPtr queryPlan);

    /**
     * @brief get the json representation of execution plan of a query
     * @param the global execution plan
     * @param id of the query
     * @return
     */
    static web::json::value getExecutionPlanAsJson(GlobalExecutionPlanPtr globalExecutionPlan, QueryId queryId = INVALID_QUERY_ID);

  private:
    /**
     * @brief function to traverse to queryPlanChildren
     * @param root root operator of the queryPlan
     * @param nodes JSON array to store the traversed node
     * @param edges JSON array to store the traversed edge
     */
    static void getChildren(const OperatorNodePtr root, std::vector<web::json::value>& nodes,
                            std::vector<web::json::value>& edges);

    /**
     * @param an operator node
     * @return the type of operator in String
     */
    static std::string getOperatorType(OperatorNodePtr operatorNode);
};
}// namespace NES
#endif//NES_PLANJSONGENERATOR_HPP
