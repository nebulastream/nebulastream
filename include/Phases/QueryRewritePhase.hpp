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

#ifndef NES_QUERYREWRITEPHASE_HPP
#define NES_QUERYREWRITEPHASE_HPP

#include <memory>

namespace NES {
class QueryRewritePhase;
typedef std::shared_ptr<QueryRewritePhase> QueryRewritePhasePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class LogicalSourceExpansionRule;
typedef std::shared_ptr<LogicalSourceExpansionRule> LogicalSourceExpansionRulePtr;

class FilterPushDownRule;
typedef std::shared_ptr<FilterPushDownRule> FilterPushDownRulePtr;

class DistributeWindowRule;
typedef std::shared_ptr<DistributeWindowRule> DistributeWindowRulePtr;

/**
 * @brief This phase is responsible for re-writing the query plan
 */
class QueryRewritePhase {
  public:
    static QueryRewritePhasePtr create(StreamCatalogPtr streamCatalog);

    /**
     * @brief Perform query plan re-write for the input query plan
     * @param queryPlan : the input query plan
     * @return updated query plan
     */
    QueryPlanPtr execute(QueryPlanPtr queryPlan);

    ~QueryRewritePhase();

  private:
    explicit QueryRewritePhase(StreamCatalogPtr streamCatalog);
    LogicalSourceExpansionRulePtr logicalSourceExpansionRule;
    FilterPushDownRulePtr filterPushDownRule;
    DistributeWindowRulePtr distributeWindowRule;
};
}// namespace NES
#endif//NES_QUERYREWRITEPHASE_HPP
