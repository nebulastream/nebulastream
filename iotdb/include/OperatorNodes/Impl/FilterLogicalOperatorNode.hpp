#ifndef FILTER_LOGICAL_OPERATOR_NODE_HPP
#define FILTER_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <API/UserAPIExpression.hpp>
#include <OperatorNodes/LogicalOperatorNode.hpp>

namespace NES {

class FilterLogicalOperatorNode : public LogicalOperatorNode {
  public:
    FilterLogicalOperatorNode() = default;
    ~FilterLogicalOperatorNode() {
        std::cout << "puu" << std::endl;
    };
    FilterLogicalOperatorNode(const PredicatePtr&);

    FilterLogicalOperatorNode(const FilterLogicalOperatorNode& other);
    FilterLogicalOperatorNode(const FilterLogicalOperatorNode* other);
    // FilterLogicalOperatorNode& operator=(FilterLogicalOperatorNode& other);

    /**
     * @brief check two filter operators are equal or not. Noting they could be
     *        two different objects with the same intra params.
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     */
    bool equal(const NodePtr& rhs) const override;
    const std::string toString() const override;
  private:
    PredicatePtr predicate_;

};

typedef std::shared_ptr<FilterLogicalOperatorNode> FilterLogicalOperatorNodePtr;

}      // namespace NES
#endif // FILTER_LOGICAL_OPERATOR_NODE_HPP
