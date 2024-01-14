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

class Node;
using NodePtr = std::shared_ptr<Node>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

/**
 * @brief This represents the plan that need to be executed on the worker node
 */
class DecomposedQueryPlan {

  public:
    /**
     * @brief Create an instance of decomposed query plan with initial state in MARKED_FOR_DEPLOYMENT
     * @param decomposedQueryPlanId: the decomposed query plan id
     * @param sharedQueryId: the shared query plan
     * @return instance of Decomposed query plan
     */
    static DecomposedQueryPlanPtr create(DecomposedQueryPlanId decomposedQueryPlanId, SharedQueryId sharedQueryId);

    /**
     * @brief Create an instance of decomposed query plan with initial state in MARKED_FOR_DEPLOYMENT
     * @param decomposedQueryPlanId: the decomposed query plan id
     * @param sharedQueryId: the shared query plan
     */
    explicit DecomposedQueryPlan(DecomposedQueryPlanId decomposedQueryPlanId, SharedQueryId sharedQueryId);

    /**
     * @brief Add the operator as new root operator
     * @param newRootOperator : new root operator
     */
    void addRootOperator(LogicalOperatorNodePtr newRootOperator);

    /**
     * @brief Remove the operator with given id as the root
     * @param rootOperatorId : the operator id
     * @return true if success else false
     */
    bool removeAsRootOperator(OperatorId rootOperatorId);

    /**
     * @brief Get all root operators
     * @return vector of root operators
     */
    std::vector<LogicalOperatorNodePtr> getRootOperators();

    /**
     * @brief Get all the leaf operators in the query plan (leaf operator is the one without any child)
     * @note: in certain stages the source operators might not be Leaf operators
     * @return returns a vector of leaf operators
     */
    std::vector<LogicalOperatorNodePtr> getLeafOperators();

    /**
     * @brief Return the id of the decomposed query plan
     * @return decomposed query plan id
     */
    DecomposedQueryPlanId getDecomposedQueryPlanId();

    /**
     * @brief Get the new decomposed query plan id
     * @param newDecomposedQueryPlanId: the new decomposed query plan id
     */
    void setDecomposedQueryPlanId(DecomposedQueryPlanId newDecomposedQueryPlanId);

    /**
     * @brief Get the shared query id
     * @return shared query id
     */
    SharedQueryId getSharedQueryId();

    /**
     * @brief Get state of the query plan
     * @return query state
     */
    QueryState getState() const;

    /**
     * @brief Set state of the query plan
     * @param newState : new decomposed query plan state
     */
    void setState(QueryState newState);

    /**
     * @brief Get version of the query plan
     * @return current version
     */
    DecomposedQueryPlanVersion getVersion() const;

    /**
     * @brief Set new version for the query sub plan
     * @param newVersion: new version of the query sub plan
     */
    void setVersion(DecomposedQueryPlanVersion newVersion);

    /**
     * @brief Get operator with input id
     * @param operatorId: the id of the operator
     * @return the shared pointer to the operator node
     */
    LogicalOperatorNodePtr getOperatorWithId(OperatorId operatorId);

    /**
     * @brief Check if the decomposed query plan contains an operator with input id
     * @param operatorId : the operator id
     * @return true if exists else false
     */
    bool hasOperatorWithId(OperatorId operatorId);

    /**
     * @brief Get all source operators
     * @return vector of logical source operators
     */
    std::vector<SourceLogicalOperatorNodePtr> getSourceOperators();

    /**
     * @brief Get all sink operators
     * @return vector of logical sink operators
     */
    std::vector<SinkLogicalOperatorNodePtr> getSinkOperators();

    /**
     * @brief Get all operators in the query plan
     * @return vector of operators
     */
    std::set<LogicalOperatorNodePtr> getAllOperators();

    /**
     * @brief Create copy of the decomposed query plan
     * @return copy
     */
    DecomposedQueryPlanPtr copy();

    /**
     * @brief String representation of the decomposed query plan
     * @return string
     */
    std::string toString() const;

  private:
    DecomposedQueryPlanId decomposedQueryPlanId;
    SharedQueryId sharedQueryId;
    QueryState currentState;
    uint32_t currentVersion;
    std::vector<LogicalOperatorNodePtr> rootOperators;
};
}// namespace NES

#endif//NES_QUERYSUBPLAN_HPP
