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

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_UTIL_PLANJSONGENERATOR_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_UTIL_PLANJSONGENERATOR_HPP_

#include <Identifiers.hpp>
#include <nlohmann/json.hpp>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

namespace Optimizer {
class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;
}// namespace Optimizer

/**
 * @brief This is a utility class to convert different plans into JSON
 */
class PlanJsonGenerator {

  public:
    static nlohmann::json getQueryPlanAsJson(const QueryPlanPtr& queryPlan);

  private:
    /**
     * @brief function to traverse to queryPlanChildren
     * @param root root operator of the queryPlan
     * @param nodes JSON array to store the traversed node
     * @param edges JSON array to store the traversed edge
     */
    static void getChildren(OperatorNodePtr const& root, std::vector<nlohmann::json>& nodes, std::vector<nlohmann::json>& edges);

    /**
     * @param an operator node
     * @return the type of operator in String
     */
    static std::string getOperatorType(const OperatorNodePtr& operatorNode);
};
}// namespace NES
#endif // NES_CATALOGS_INCLUDE_CATALOGS_UTIL_PLANJSONGENERATOR_HPP_
