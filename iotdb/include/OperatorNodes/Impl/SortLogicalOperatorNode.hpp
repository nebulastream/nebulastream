#ifndef SORT_LOGICAL_OPERATOR_NODE_HPP
#define SORT_LOGICAL_OPERATOR_NODE_HPP

#include <OperatorNodes/BaseOperatorNode.hpp>

namespace NES {

class SortLogicalOperatorNode : public BaseOperatorNode {
  public:

    SortLogicalOperatorNode(const Sort& sort_spec);
    const std::string toString() const override;
    OperatorType getOperatorType() const override;

    virtual bool equals(const BaseOperatorNode& rhs) const override;
  private:
    Sort sort_spec_;
};

} // namespace NES
#endif // SORT_LOGICAL_OPERATOR_NODE_HPP
