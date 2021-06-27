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

#ifndef NES_INCLUDE_PLANS_QUERYRECONFIGURATIONPLAN_HPP_
#define NES_INCLUDE_PLANS_QUERYRECONFIGURATIONPLAN_HPP_
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QueryReconfigurationId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <memory>
#include <ostream>
#include <set>
#include <unordered_map>
#include <vector>

namespace NES {

class QueryReconfigurationPlan;
typedef std::shared_ptr<QueryReconfigurationPlan> QueryReconfigurationPlanPtr;

enum QueryReconfigurationTypes { DATA_SOURCE, DATA_SINK };

class QueryReconfigurationPlan {
  public:
    QueryReconfigurationPlan(const QueryReconfigurationId reconfigurationId,
                             const QueryId queryId,
                             const QueryReconfigurationTypes reconfigurationType,
                             const QuerySubPlanId oldQuerySubPlanId,
                             const QuerySubPlanId newQuerySubPlanId);

    static QueryReconfigurationPlanPtr create(const QueryId queryId,
                                              const QueryReconfigurationTypes reconfigurationType,
                                              const QuerySubPlanId oldQuerySubPlanId,
                                              const QuerySubPlanId newQuerySubPlanId);

    /**
     * @brief Get QuerySubPlanId to replace
     * @return QuerySubPlanId
     */
    const QuerySubPlanId getOldQuerySubPlanId() const;

    /**
     * @brief Get QuerySubPlanId to replace by
     * @return QuerySubPlanId
     */
    const QuerySubPlanId getNewQuerySubPlanId() const;

    QueryReconfigurationTypes getReconfigurationType() const;

    /**
     * Get query ID targeted via reconfiguration
     * @return queryId
     */
    QueryId getQueryId() const;

    /**
     * Get unique ID for reconfiguration
     * @return queryReconfigurationId
     */
    QueryReconfigurationId getId() const;

    std::string toString();

  private:
    QueryReconfigurationId id;
    QueryId queryId;
    QueryReconfigurationTypes reconfigurationType;
    QuerySubPlanId oldQuerySubPlanId;
    QuerySubPlanId newQuerySubPlanId;
};
}// namespace NES
#endif//NES_INCLUDE_PLANS_QUERYRECONFIGURATIONPLAN_HPP_
