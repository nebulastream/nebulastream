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

#ifndef NES_INCLUDE_PLANS_GLOBAL_QUERY_SHAREDQUERYPLAN_HPP_
#define NES_INCLUDE_PLANS_GLOBAL_QUERY_SHAREDQUERYPLAN_HPP_

#include <Plans/Global/Query/SharedQueryId.hpp>
#include <Plans/Query/QueryId.hpp>
#include <memory>
#include <set>
#include <vector>

namespace NES {

class Node;
using NodePtr = std::shared_ptr<Node>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class SharedQueryPlanChangeLog;
using SharedQueryPlanChangeLogPtr = std::shared_ptr<SharedQueryPlanChangeLog>;

class SharedQueryPlan;
using SharedQueryPlanPtr = std::shared_ptr<SharedQueryPlan>;

/**
 * @brief This class holds a query plan shared by multiple queries i.e. from its source nodes we can reach the sink nodes of all
 * the queries participating in the shared query plan. A Global Query Plan can consists of multiple Shared Query Plans.
 * Additionally, a query con share only one Shared Query Plan within a Global Query Plan.
 *
 * Example:
 *
 * Example Query Plan:
 *
 *               {Q1}             {Q2}
 *             (Sink1)           (Sink2)
 *                |                 |
 *             (Map1)            (Map1)
 *                |                 |
 *            (Filter1)         (Filter1)
 *               |                 |
 *         (Source(Car))      (Source(Car))
 *
 * Shared Query Plan:
 *
 * sharedQueryPlanId: 1
 * queryPlan:
 *             (Sink1)       (Sink2)
 *                \          /
 *                  \      /
 *                     |
 *                  (Map1)
 *                    |
 *                (Filter1)
 *                   |
 *             (Source(Car))
 *
 * sharedQueryPlanId:
 *
 * queryIdToSinkOperatorMap:  {Q1:Sink1,Q2:Sink2}
 * queryIds: [Q1, Q2]
 * sinkOperators: [Sink1, Sink2]
 * deployed: false
 * newMetaData: true
 *
 * In the above Shared query plan, we have two QueryPlans with Sink1 and sink2 as respective sink nodes.
 * A SharedQueryPlan consists of a unique:
 *
 * - sharedQueryPlanId : this id is equivalent to the Query Id assigned to the user queryId. Since, there can be more than one query that can be merged
 *                      together we generate a unique Shared Query Id that can be associated to more than one Query Ids.
 * - queryPlan : the query plan for the shared query plan
 * - queryIdToSinkOperatorMap: the mapping among the query and the sink operator
 * - queryIds : A vector of original Query Ids participating in the shared query plan
 * - sinkOperators: A vector of sink operators in the query plan.
 * - deployed : A boolean flag indicating if the query plan is deployed or not.
 * - newMetaData : A boolean flag indicating if the meta data is a newly created one (i.e. it was never deployed before).
 *
 */
class SharedQueryPlan {

  public:
    static SharedQueryPlanPtr create(QueryPlanPtr queryPlan);

    /**
     * @brief Remove a Query, the associated exclusive operators, and clear sink and query id vectors
     * @param queryId : the original query Id
     * @return true if successful
     */
    bool removeQuery(QueryId queryId);

    /**
     * @brief Add the addition information to the change log
     * @param upstreamOperator: the upstream operator to which operator needs to be added
     * @param newOperator : the newly added operator
     */
    void addAdditionToChangeLog(const OperatorNodePtr& upstreamOperator, const OperatorNodePtr& newOperator);

    /**
     * @brief Clear all MetaData information
     */
    void clear();

    /**
     * @brief Get the Query plan for deployment build from the information within this metadata
     * @return the query plan with all the interconnected logical operators
     */
    QueryPlanPtr getQueryPlan();

    /**
     * @brief Mark the global query as deployed
     */
    void markAsDeployed();

    /**
     * @brief Mark the global query as not deployed
     */
    void markAsNotDeployed();

    /**
     * @brief Is the query plan for the metadata deployed
     * @return true if deployed else false
     */
    [[nodiscard]] bool isDeployed() const;

    /**
     * @brief Check if the metadata is empty
     * @return true if vector of queryIds is empty
     */
    bool isEmpty();

    /**
     * @brief Check if the metadata is newly created
     * A New metadata is the one which was created by never got deployed before
     * @return true if newly created else false
     */
    [[nodiscard]] bool isNew() const;

    /**
     * @brief Get the vector of sink operators sharing common upstream operators
     * @return the vector of Sink Operators
     */
    std::vector<OperatorNodePtr> getSinkOperators();

    /**
     * @brief Get the collection of registered query ids and their sink operators
     * @return map of registered query ids and their sink operators
     */
    std::map<QueryId, std::vector<OperatorNodePtr>> getQueryIdToSinkOperatorMap();

    /**
     * @brief Get the shared query id
     * @return shared query id
     */
    [[nodiscard]] SharedQueryId getSharedQueryId() const;

    /**
     * @brief Get all query ids part of the SharedQueryPlan
     * @return vector of query ids
     */
    std::vector<QueryId> getQueryIds();

    /**
     * @brief Set the meta data as old
     * An Old metadata is deployed at least once in his life time.
     */
    void setAsOld();

    /**
     * @brief Add the query id and sink operators from the query plan to the Shared Query Plan
     * @param queryPlan: the source query plan
     * @return true if successful else false
     */
    bool addQueryIdAndSinkOperators(const QueryPlanPtr& queryPlan);

    /**
     * @brief Get the shared query plan change log since the last time the shared query plan was deployed.
     * @return the change log
     */
    SharedQueryPlanChangeLogPtr getChangeLog();

    /**
     * @brief Get the hash based signature for the shared query plan
     * @return collection of hash based signatures
     */
    std::map<size_t, std::set<std::string>> getHashBasedSignature();

    /**
     * @brief Update the hash based signatures with new values
     * @param hashValue: The hash value
     * @param stringSignature: The string signature
     */
    void updateHashBasedSignature(size_t hashValue, const std::string& stringSignature);

  private:
    explicit SharedQueryPlan(const QueryPlanPtr& queryPlan);

    /**
     * @brief Recursively remove the operator and all its children operators that have no other downstream operator
     * @param operatorToRemove : the operator to remove
     * @return true if successful else false
     */
    bool removeOperator(const OperatorNodePtr& operatorToRemove);

    SharedQueryId sharedQueryId;
    QueryPlanPtr queryPlan;
    std::map<QueryId, std::vector<OperatorNodePtr>> queryIdToSinkOperatorMap;
    std::vector<QueryId> queryIds;
    std::vector<OperatorNodePtr> sinkOperators;
    SharedQueryPlanChangeLogPtr changeLog;
    bool deployed;
    bool newMetaData;
    //FIXME: #2274 We have to figure out a way to change it once a query is removed
    std::map<size_t, std::set<std::string>> hashBasedSignatures;
};
}// namespace NES

#endif  // NES_INCLUDE_PLANS_GLOBAL_QUERY_SHAREDQUERYPLAN_HPP_
