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

#ifndef NES_OPERATORS_INCLUDE_PLANS_DECOMPOSEDQUERYPLAN_DECOMPOSEDQUERYPLAN_HPP_
#define NES_OPERATORS_INCLUDE_PLANS_DECOMPOSEDQUERYPLAN_DECOMPOSEDQUERYPLAN_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Operators/Operator.hpp>
#include <Util/QueryState.hpp>
#include <memory>
#include <optional>
#include <set>
#include <unordered_set>
#include <vector>

namespace NES {

class Operator;
using OperatorPtr = std::shared_ptr<Operator>;

class SourceLogicalOperator;
using SourceLogicalOperatorPtr = std::shared_ptr<SourceLogicalOperator>;

class SinkLogicalOperator;
using SinkLogicalOperatorPtr = std::shared_ptr<SinkLogicalOperator>;

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
     * @param decomposedQueryId: the decomposed query plan id
     * @param sharedQueryId: the shared query plan
     * @param workerId: the worker id
     * @return instance of Decomposed query plan
     */
    static DecomposedQueryPlanPtr create(DecomposedQueryId decomposedQueryId, SharedQueryId sharedQueryId, WorkerId workerId);

    /**
     * @brief Create an instance of decomposed query plan with initial state in MARKED_FOR_DEPLOYMENT
     * @param decomposedQueryId: the decomposed query plan id
     * @param sharedQueryId: the shared query id
     * @param workerId: the worker id
     * @param rootOperators: the root operators
     * @return instance of Decomposed query plan
     */
    static DecomposedQueryPlanPtr create(DecomposedQueryId decomposedQueryId,
                                         SharedQueryId sharedQueryId,
                                         WorkerId workerId,
                                         std::vector<OperatorPtr> rootOperators);

    /**
     * @brief Create an instance of decomposed query plan with initial state in MARKED_FOR_DEPLOYMENT
     * @param decomposedQueryId: the decomposed query plan id
     * @param sharedQueryId: the shared query id
     * @param workerId: the worker id
     */
    explicit DecomposedQueryPlan(DecomposedQueryId decomposedQueryId, SharedQueryId sharedQueryId, WorkerId workerId);

    /**
     * @brief Create an instance of decomposed query plan with initial state in MARKED_FOR_DEPLOYMENT
     * @param decomposedQueryId: the decomposed query plan id
     * @param sharedQueryId: the shared query id
     * @param workerId: the worker id
     * @param rootOperators: the root operators
     */
    explicit DecomposedQueryPlan(DecomposedQueryId decomposedQueryId,
                                 SharedQueryId sharedQueryId,
                                 WorkerId workerId,
                                 std::vector<OperatorPtr> rootOperators);

    /**
     * @brief Add the operator as new root operator
     * @param newRootOperator : new root operator
     */
    void addRootOperator(OperatorPtr newRootOperator);

    /**
     * @brief Remove the operator with given id as the root
     * @param rootOperatorId : the operator id
     * @return true if success else false
     */
    bool removeAsRootOperator(OperatorId rootOperatorId);

    /**
     * replaces a particular root operator with a new one.
     * @param root
     * @return true if operator was replaced.
     */
    bool replaceRootOperator(const OperatorPtr& oldRoot, const OperatorPtr& newRoot);

    /**
     * @brief Appends an operator to the query plan and make the new operator as root.
     * @param operatorNode : new operator
     */
    void appendOperatorAsNewRoot(const OperatorPtr& operatorNode);

    /**
     * @brief Get all root operators
     * @return vector of root operators
     */
    std::vector<OperatorPtr> getRootOperators() const;

    /**
     * @brief Get all the leaf operators in the query plan (leaf operator is the one without any child)
     * @note: in certain stages the source operators might not be Leaf operators
     * @return returns a vector of leaf operators
     */
    std::vector<OperatorPtr> getLeafOperators() const;

    /**
     * @brief Return the id of the decomposed query plan
     * @return decomposed query plan id
     */
    DecomposedQueryId getDecomposedQueryId() const;

    /**
     * @brief Get the new decomposed query plan id
     * @param newDecomposedQueryPlanId: the new decomposed query plan id
     */
    void setDecomposedQueryPlanId(DecomposedQueryId newDecomposedQueryPlanId);

    /**
     * @brief Get the shared query id
     * @return shared query id
     */
    SharedQueryId getSharedQueryId() const;

    /**
     * @brief Get state of the query plan
     * @return query state
     */
    QueryState getState() const;

    /**
     * @brief refresh the operator ids and put the original ids to the properties
     */
    void refreshOperatorIds();

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
     * @brief Get the worker id
     * @return worker id
     */
    WorkerId getWorkerId() const;

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
    OperatorPtr getOperatorWithOperatorId(OperatorId operatorId) const;

    /**
     * @brief Check if the decomposed query plan contains an operator with input id
     * @param operatorId : the operator id
     * @return true if exists else false
     */
    bool hasOperatorWithId(OperatorId operatorId) const;

    /**
     * @brief Get all source operators
     * @return vector of logical source operators
     */
    std::vector<SourceLogicalOperatorPtr> getSourceOperators() const;

    /**
     * @brief Get all sink operators
     * @return vector of logical sink operators
     */
    std::vector<SinkLogicalOperatorPtr> getSinkOperators() const;

    /**
     * @brief Get all operators in the query plan
     * @return vector of operators
     */
    std::unordered_set<OperatorPtr> getAllOperators() const;

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

    /**
     * @brief Record the previous version of this plan that can be used to identfify the running instance on the worker side
     */
    void setOldVersion(DecomposedQueryPlanVersion oldVersion);

    /**
     * @brief Get the previous version of this plan that can be used to identfify the running instance on the worker side
     * @return an optional either containing the old version or nullopt if not old version was set
     */
    std::optional<DecomposedQueryPlanVersion> getOldVersion() const;

    void setPlanForMigration(bool isMigration);
    bool isPlanForMigration();

    /**
     * @brief Get all the operators of a specific type
     * @return returns a vector of operators
     */
    template<class T>
    std::vector<std::shared_ptr<T>> getOperatorByType() {
        // Find all the nodes in the query plan
        std::vector<std::shared_ptr<T>> operators;
        // Maintain a list of visited nodes as there are multiple root nodes
        std::set<OperatorId> visitedOpIds;
        for (const auto& rootOperator : rootOperators) {
            auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
            for (auto itr = bfsIterator.begin(); itr != NES::BreadthFirstNodeIterator::end(); ++itr) {
                auto visitingOp = (*itr)->as<Operator>();
                if (visitedOpIds.contains(visitingOp->getId())) {
                    // skip rest of the steps as the node found in already visited node list
                    continue;
                }
                visitedOpIds.insert(visitingOp->getId());
                if (visitingOp->instanceOf<T>()) {
                    operators.push_back(visitingOp->as<T>());
                }
            }
        }
        return operators;
    }

  private:
    SharedQueryId sharedQueryId;
    DecomposedQueryId decomposedQueryId;
    DecomposedQueryPlanVersion decomposedQueryPlanVersion;
    std::optional<DecomposedQueryPlanVersion> oldVersion;
    WorkerId workerId;
    QueryState currentState = QueryState::MARKED_FOR_DEPLOYMENT;
    std::vector<OperatorPtr> rootOperators;
    bool isMigrationPlan = false;
};
}// namespace NES

#endif// NES_OPERATORS_INCLUDE_PLANS_DECOMPOSEDQUERYPLAN_DECOMPOSEDQUERYPLAN_HPP_
