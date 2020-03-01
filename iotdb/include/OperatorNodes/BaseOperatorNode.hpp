#ifndef BASE_OPERATOR_NODE_HPP
#define BASE_OPERATOR_NODE_HPP

#include <iostream>
#include <vector>
#include <memory>
#include <Operators/Operator.hpp>
#include <API/Schema.hpp>
#include <API/ParameterTypes.hpp>
#include <API/AbstractWindowDefinition.hpp>

namespace NES {

class BaseOperatorNode;

typedef std::shared_ptr<BaseOperatorNode> BaseOperatorNodePtr;


class BaseOperatorNode {
public:
    BaseOperatorNode();
    ~BaseOperatorNode();
    // BaseOperatorNode& operator=(BaseOperatorNode& other);
    // template<typename T>
    // T& operator=(T& other);
    /**
     * @brief add a successor to vector of successors
     *        no duplicated operator inside successors.
     *        one cannot add current operator into its successors
     * @param op
     */
    void addSuccessor(const BaseOperatorNodePtr& op);
    /**
     * @brief remove a successor from vector of successors.
     * @param op
     */
    bool removeSuccessor(const BaseOperatorNodePtr& op);
    /**
     * @brief add a predecessor to vector of predecessors
     *        no duplicated operator inside predecessors.
     *        one cannot add current operator into its predecessors.
     * @param op
     */
    void addPredecessor(const BaseOperatorNodePtr& op);
    /**
     * @brief remove a predecessor from vector of predecessors
     * @param op
     */
    bool removePredecessor(const BaseOperatorNodePtr& op);
    /**
     * @brief replace an old opertoar with new operator
     * 1) oldOperator is the successor of current operator, remove oldOperator
     *    from current operator's successors and add newOperator to current operator's
     *    successors. If oldOperator has successors, merge the successors of oldOperator
     *    and newOperator's. If there's duplicated successors among oldOperator and
     *    newOperator's successors, the successors in newOperator will overwrite that
     *    inside oldOperator's.
     * 2)
     * @param newOperator
     * @param oldOperator
     */
    bool replace(BaseOperatorNodePtr newOperator, BaseOperatorNodePtr oldOperator);

    /**
     * @brief remove the given operator together with its successors
     * @param op the given operator to remove
     * @return bool true if
     */
    bool remove(const BaseOperatorNodePtr& op);
    /**
     * @brief remove the given operator and level up its successors
     * @param op

     * @return bool true if
     */
    bool removeAndLevelUpSuccessors(const BaseOperatorNodePtr& op);
    /**
     * @brief clear all predecessors and successors
     */
    void clear();

    virtual OperatorType getOperatorType() const = 0;
    virtual const std::string toString() const = 0;

    // size_t getOperatorId() const;
    // void setOperatorId(const size_t id);

    const std::string getOperatorId() const;
    void setOperatorId(const std::string& id);

    const std::vector<BaseOperatorNodePtr>& getSuccessors() const;
    const std::vector<BaseOperatorNodePtr>& getPredecessors() const;

    bool equalWithAllSuccessors(const BaseOperatorNodePtr& rhs);
    bool equalWithAllSuccessorsHelper(const BaseOperatorNode& op1, const BaseOperatorNode& op2);
    bool equalWithAllPredecessors(const BaseOperatorNodePtr& rhs);
    bool equalWithAllPredecessorsHelper(const BaseOperatorNode& op1, const BaseOperatorNode& op2);
    // bool _equalWithAll(const BaseOperatorNodePtr& rhs);
    // TODO: make it pure virtual

    /**
     * @brief check two operators are equal or not. Noting they could be
     *        two different objects with the same predicates.
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     */
    virtual bool equals(const BaseOperatorNode& rhs) const { return true; };
    /**
     * @brief overload operator==, same as equals() function
     */
    virtual bool operator==(const BaseOperatorNode& rhs) const { return this->equals(rhs); };
    virtual bool operator!=(const BaseOperatorNode& rhs) const { return ! this->equals(rhs); };
    /**
     * @brief check two operators whether are exactly the same object or not
     * @param rhs the operator to check
     * @return bool true if they are the same object otherwise false
     */
    virtual bool isIdentical(const BaseOperatorNode& rhs) const { return &rhs == this; };
    virtual bool isNotIdentical(const BaseOperatorNode& rhs) const { return &rhs != this; };

protected:
    const std::string operatorId;
    std::vector<BaseOperatorNodePtr> predecessors {};
    std::vector<BaseOperatorNodePtr> successors {};
private:
    /**
     * @brief check if an operator is in given vector or not
     * @param operatorNodes
     * @param op
     * @return return true if the operator is found, otherwise false
     */
    bool find(const std::vector<BaseOperatorNodePtr>& operatorNodes, const BaseOperatorNodePtr& op);

};

const BaseOperatorNodePtr createAggregationLogicalOperatorNode(const AggregationSpec& aggrSpec);
const BaseOperatorNodePtr createFilterLogicalOperatorNode(const PredicatePtr& predicate);
const BaseOperatorNodePtr createJoinLogicalOperatorNode(const JoinPredicatePtr& joinSpec);
const BaseOperatorNodePtr createKeyByLogicalOperatorNode(const Attributes& keybySpec);
const BaseOperatorNodePtr createMapLogicalOperatorNode(const AttributeFieldPtr&, const PredicatePtr&);
const BaseOperatorNodePtr createSinkLogicalOperatorNode(const DataSinkPtr& sink);
const BaseOperatorNodePtr createSourceLogicalOperatorNode(const DataSourcePtr& source);
const BaseOperatorNodePtr createSortLogicalOperatorNode(const Sort& sortSpec);
const BaseOperatorNodePtr createWindowLogicalOperatorNode(const WindowDefinitionPtr& windowDefinition);
//  BaseOperatorNodePtr createPlaceholderLogicalOperatorNode();

}      // namespace NES

#endif  // BASE_OPERATOR_NODE_HPP
