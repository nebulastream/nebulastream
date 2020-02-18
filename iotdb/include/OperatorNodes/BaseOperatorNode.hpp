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
    template<typename T>
    T& operator=(T& other);
    /**
     * @brief add a successor to vector of successors
     * @param baseOperatorNode
     */
    void addSuccessor(BaseOperatorNodePtr baseOperatorNode);
    /**
     * @brief remove a successor from vector of successors
     * @param baseOperatorNode
     */
    bool removeSuccessor(BaseOperatorNodePtr baseOperatorNode);
    /**
     * @brief add a predecessor to vector of predecessors
     * @param baseOperatorNode
     */
    void addPredecessor(BaseOperatorNodePtr baseOperatorNode);
    /**
     * @brief remove a predecessor from vector of predecessors
     * @param baseOperatorNode
     */
    bool removePredecessor(BaseOperatorNodePtr baseOperatorNode);
    /**
     * @brief replace an old opertoar with new operator
     * @param newOperator
     * @param oldOperator
     */
    bool replace(BaseOperatorNodePtr newOperator, BaseOperatorNodePtr oldOperator);

    bool remove(BaseOperatorNodePtr baseOperatorNode);
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

protected:
    // size_t operatorId;
    std::string operatorId;
    std::vector<BaseOperatorNodePtr> predecessors {};
    std::vector<BaseOperatorNodePtr> successors {};
private:


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
