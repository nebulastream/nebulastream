#ifndef FILTER_LOGICAL_OPERATOR_NODE_HPP
#define FILTER_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <API/UserAPIExpression.hpp>
#include <OperatorNodes/LogicalOperatorNode.hpp>


namespace NES {

class FilterLogicalOperatorNode : public LogicalOperatorNode,
                                  public std::enable_shared_from_this<FilterLogicalOperatorNode> {
public:
    FilterLogicalOperatorNode() = default;
    FilterLogicalOperatorNode(const PredicatePtr&);

    FilterLogicalOperatorNode(const FilterLogicalOperatorNode& other);
    // FilterLogicalOperatorNode& operator=(FilterLogicalOperatorNode& other);

    /**
     * @brief check two filter operators are equal or not. Noting they could be
     *        two different objects with the same intra params.
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     */
    virtual bool equals(const Node& rhs) const override;
    virtual NodePtr makeShared() override;
    OperatorType getOperatorType() const override;
    const std::string toString() const override;
private:
    PredicatePtr predicate_;

};

}      // namespace NES
#endif // FILTER_LOGICAL_OPERATOR_NODE_HPP
