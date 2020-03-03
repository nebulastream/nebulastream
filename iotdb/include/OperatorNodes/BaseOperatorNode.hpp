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

class FilterLogicalOperatorNode;
class BaseOperatorNode;

typedef std::shared_ptr<BaseOperatorNode> BaseOperatorNodePtr;

/**
todo rename to Node.
*/
class BaseOperatorNode {
public:
    BaseOperatorNode();
    ~BaseOperatorNode();
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
     * @brief swap given old operator by new operator
     * @param newOp the operator to mount at oldOp predecessors instead of oldOp
     * @param oldOp the operator to remove from graph
     * @return true if swapping successfully otherwise false
     */
    bool swap(const BaseOperatorNodePtr& newOp, const BaseOperatorNodePtr& oldOp);

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

    /**
     * @brief get the id of an operator
     * @return string
     */
    const std::string getOperatorId() const;
    void setOperatorId(const std::string& id);

    const std::vector<BaseOperatorNodePtr>& getSuccessors() const;
    const std::vector<BaseOperatorNodePtr>& getPredecessors() const;

    bool equalWithAllSuccessors(const BaseOperatorNodePtr& rhs);
    bool equalWithAllPredecessors(const BaseOperatorNodePtr& rhs);

    /**
     * @brief check two operators are equal or not. Noting they could be
     *        two different objects with the same predicates.
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     * @TODO should be a pure virtual function
     */
    virtual bool equals(const BaseOperatorNode& rhs) const { return true; };
    /**
     * @brief overload operator==, same as equals() function
     */
    virtual bool operator==(const BaseOperatorNode& rhs) const { return this->equals(rhs); };
    /**
     * @brief overload operator!=
     */
    virtual bool operator!=(const BaseOperatorNode& rhs) const { return ! this->equals(rhs); };
    /**
     * @brief check two operators whether are exactly the same object or not
     * @param rhs the operator to check
     * @return bool true if they are the same object otherwise false
     */
    virtual bool isIdentical(const BaseOperatorNode& rhs) const { return &rhs == this; };
    /**
     * @see isIdentical() function
     */
    virtual bool isNotIdentical(const BaseOperatorNode& rhs) const { return &rhs != this; };

//    void getOperatorsByType(const OperatorType& type, std::vector<BaseOperatorNodePtr>& vec);
    std::vector<BaseOperatorNodePtr> getOperatorsByType(const OperatorType& type);
    /**
     * @brief split graph into multiple sub-graphs. The graph starts at current operator
     *        if the given operator is not in the graph, throw exception
     * @params op the given operator to split at.
     * @return vector of multiple sub-graphs.
     */
    std::vector<BaseOperatorNodePtr> split(const BaseOperatorNodePtr& op);

    /**
     * @brief validation of this operator
     * @return true if there is no ring/loop inside this operator's successors, otherwise false
     */
    bool isValid();
    bool instanceOf(const OperatorType& type);

    template<class T>
    T& as() {
        T& rhs = dynamic_cast<T&>(*this);
        return rhs;
    }
    /**
     * @brief obtain the shared_ptr of this instance
     */
    virtual BaseOperatorNodePtr makeShared() = 0;

    bool isCyclic();

    /**
     * @brief return all successors of current operator
     *        Always excluding current operator, no matter a cycle exists
     * @params allChildren a vector to store all successors of current operator
     */
    std::vector<BaseOperatorNodePtr> getAndFlattenAllSuccessors();

    void prettyPrint(std::ostream& out=std::cout) const;
protected:
    /**
     * @brief the operator id would be set as uuid to ensure uniqueness
     *        the main reason is that we'd like make sure all operators in
     *        the given graph (tree) should be unique.
     */
    const std::string operatorId;
    /**
     * @brief the predecessors of this operator. There is no equal operators
     *        in this vector
     */
    std::vector<BaseOperatorNodePtr> predecessors {};
    /**
     * @brief the successors of this operator. There is no equal operators
     *        in this vector
     */
    std::vector<BaseOperatorNodePtr> successors {};
private:
    /**
     * @brief check if an operator is in given vector or not
     * @param operatorNodes
     * @param op
     * @return return true if the given operator is found, otherwise false
     */
    BaseOperatorNodePtr find(const std::vector<BaseOperatorNodePtr>& operatorNodes, const BaseOperatorNodePtr& op);
    /**
     * @brief check if an operator is in given graph
     * @param root
     * @param op
     * @return return true if the given operator is found in the graph of root, otherwise false
     */
    BaseOperatorNodePtr findRecursively(BaseOperatorNode& root, BaseOperatorNode& op);


    /********************************************************************************
     *                   Helper functions                                           *
     ********************************************************************************/
    /**
     * @brief helper function of equalWithAllPredecessors() function
     */
    bool equalWithAllPredecessorsHelper(const BaseOperatorNode& op1, const BaseOperatorNode& op2);
    /**
     * @brief helper function of equalWithAllSuccessors() function
     */
    bool equalWithAllSuccessorsHelper(const BaseOperatorNode& op1, const BaseOperatorNode& op2);
    /**
     * @brief helper function of prettyPrint() function
     */
    void printHelper(const BaseOperatorNode& op, size_t depth, size_t indent, std::ostream& out) const;
    /**
     * @brief helper function of getAndFlattenAllSuccessors() function
     */
    void getAndFlattenAllSuccessorsHelper(BaseOperatorNode& op,
                                          std::vector<BaseOperatorNodePtr>& allChildren,
                                          BaseOperatorNode& excludedOp);

    /**
     *
     */
    void getOperatorsByTypeHelper(BaseOperatorNode& op,
                                  std::vector<BaseOperatorNodePtr>& allChildren,
                                  BaseOperatorNode& excludedOp,
                                  const OperatorType& type);

                                          // bool predicateFunc(std::vector<BaseOperatorNodePtr>&,
                                          //                    BaseOperatorNodePtr& op,
                                          //                    BaseOperatorNode& excludedOp,
                                          //                    OperatorType& type));
    // bool predicateFunc(std::vector<BaseOperatorNodePtr>& allChildren, BaseOperatorNodePtr& op, BaseOperatorNode& excludedOp, OperatorType& type) {
    //     return (!find(allChildren, op) && (op.get() != &excludedOp));
    // }

    /**
     * @brief helper function of cycle detector
     */
    bool isCyclicHelper(BaseOperatorNode& op);

    /********************************************************************************
     *                   Helper parameters                                           *
     ********************************************************************************/
    /**
     * Helper parameters for cycle detection
     */
public:
    bool visited;
    bool recStack;
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

}      // namespace NES

#endif  // BASE_OPERATOR_NODE_HPP
