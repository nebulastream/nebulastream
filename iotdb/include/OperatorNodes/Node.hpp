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
class Node;

typedef std::shared_ptr<Node> NodePtr;

class Node {
  public:
    Node();
    ~Node();
    /**
     * @brief add a successor to vector of successors
     *        no duplicated operator inside successors.
     *        one cannot add current operator into its successors
     * @param op
     */
    void addSuccessor(const NodePtr& op);
    /**
     * @brief remove a successor from vector of successors.
     * @param op
     */
    bool removeSuccessor(const NodePtr& op);
    /**
     * @brief add a predecessor to vector of predecessors
     *        no duplicated operator inside predecessors.
     *        one cannot add current operator into its predecessors.
     * @param op
     */
    void addPredecessor(const NodePtr& op);
    /**
     * @brief remove a predecessor from vector of predecessors
     * @param op
     */
    bool removePredecessor(const NodePtr& op);
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
    bool replace(NodePtr newOperator, NodePtr oldOperator);

    /**
     * @brief swap given old operator by new operator
     * @param newOp the operator to mount at oldOp predecessors instead of oldOp
     * @param oldOp the operator to remove from graph
     * @return true if swapping successfully otherwise false
     */
    bool swap(const NodePtr& newOp, const NodePtr& oldOp);

    /**
     * @brief remove the given operator together with its successors
     * @param op the given operator to remove
     * @return bool true if
     */
    bool remove(const NodePtr& op);
    /**
     * @brief remove the given operator and level up its successors
     * @param op

     * @return bool true if
     */
    bool removeAndLevelUpSuccessors(const NodePtr& op);
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

    const std::vector<NodePtr>& getSuccessors() const;
    const std::vector<NodePtr>& getPredecessors() const;

    bool equalWithAllSuccessors(const NodePtr& rhs);
    bool equalWithAllPredecessors(const NodePtr& rhs);

    /**
     * @brief check two operators are equal or not. Noting they could be
     *        two different objects with the same predicates.
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     * @TODO should be a pure virtual function
     */
    virtual bool equals(const Node& rhs) const { return true; };
    /**
     * @brief overload operator==, same as equals() function
     */
    virtual bool operator==(const Node& rhs) const { return this->equals(rhs); };
    /**
     * @brief overload operator!=
     */
    virtual bool operator!=(const Node& rhs) const { return !this->equals(rhs); };
    /**
     * @brief check two operators whether are exactly the same object or not
     * @param rhs the operator to check
     * @return bool true if they are the same object otherwise false
     */
    virtual bool isIdentical(const Node& rhs) const { return &rhs == this; };
    /**
     * @see isIdentical() function
     */
    virtual bool isNotIdentical(const Node& rhs) const { return &rhs != this; };

    //    void getOperatorsByType(const OperatorType& type, std::vector<NodePtr>& vec);
    std::vector<NodePtr> getOperatorsByType(const OperatorType& type);
    /**
     * @brief split graph into multiple sub-graphs. The graph starts at current operator
     *        if the given operator is not in the graph, throw exception
     * @params op the given operator to split at.
     * @return vector of multiple sub-graphs.
     */
    std::vector<NodePtr> split(const NodePtr& op);

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
    virtual NodePtr makeShared() = 0;

    bool isCyclic();

    /**
     * @brief return all successors of current operator
     *        Always excluding current operator, no matter a cycle exists
     * @params allChildren a vector to store all successors of current operator
     */
    std::vector<NodePtr> getAndFlattenAllSuccessors();

    void prettyPrint(std::ostream& out = std::cout) const;
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
    std::vector<NodePtr> predecessors{};
    /**
     * @brief the successors of this operator. There is no equal operators
     *        in this vector
     */
    std::vector<NodePtr> successors{};
  private:
    /**
     * @brief check if an operator is in given vector or not
     * @param operatorNodes
     * @param op
     * @return return true if the given operator is found, otherwise false
     */
    NodePtr find(const std::vector<NodePtr>& operatorNodes, const NodePtr& op);
    /**
     * @brief check if an operator is in given graph
     * @param root
     * @param op
     * @return return true if the given operator is found in the graph of root, otherwise false
     */
    NodePtr findRecursively(Node& root, Node& op);


    /********************************************************************************
     *                   Helper functions                                           *
     ********************************************************************************/
    /**
     * @brief helper function of equalWithAllPredecessors() function
     */
    bool equalWithAllPredecessorsHelper(const Node& op1, const Node& op2);
    /**
     * @brief helper function of equalWithAllSuccessors() function
     */
    bool equalWithAllSuccessorsHelper(const Node& op1, const Node& op2);
    /**
     * @brief helper function of prettyPrint() function
     */
    void printHelper(const Node& op, size_t depth, size_t indent, std::ostream& out) const;
    /**
     * @brief helper function of getAndFlattenAllSuccessors() function
     */
    void getAndFlattenAllSuccessorsHelper(Node& op,
                                          std::vector<NodePtr>& allChildren,
                                          Node& excludedOp);

    /**
     *
     */
    void getOperatorsByTypeHelper(Node& op,
                                  std::vector<NodePtr>& allChildren,
                                  Node& excludedOp,
                                  const OperatorType& type);

    // bool predicateFunc(std::vector<NodePtr>&,
    //                    NodePtr& op,
    //                    Node& excludedOp,
    //                    OperatorType& type));
    // bool predicateFunc(std::vector<NodePtr>& allChildren, NodePtr& op, Node& excludedOp, OperatorType& type) {
    //     return (!find(allChildren, op) && (op.get() != &excludedOp));
    // }

    /**
     * @brief helper function of cycle detector
     */
    bool isCyclicHelper(Node& op);

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

const NodePtr createAggregationLogicalOperatorNode(const AggregationSpec& aggrSpec);
const NodePtr createFilterLogicalOperatorNode(const PredicatePtr& predicate);
const NodePtr createJoinLogicalOperatorNode(const JoinPredicatePtr& joinSpec);
const NodePtr createKeyByLogicalOperatorNode(const Attributes& keybySpec);
const NodePtr createMapLogicalOperatorNode(const AttributeFieldPtr&, const PredicatePtr&);
const NodePtr createSinkLogicalOperatorNode(const DataSinkPtr& sink);
const NodePtr createSourceLogicalOperatorNode(const DataSourcePtr& source);
const NodePtr createSortLogicalOperatorNode(const Sort& sortSpec);
const NodePtr createWindowLogicalOperatorNode(const WindowDefinitionPtr& windowDefinition);

}      // namespace NES

#endif  // BASE_OPERATOR_NODE_HPP
