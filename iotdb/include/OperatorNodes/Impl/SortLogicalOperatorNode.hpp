#ifndef SORT_LOGICAL_OPERATOR_NODE_HPP
#define SORT_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <OperatorNodes/BaseOperatorNode.hpp>

namespace NES {

class SortLogicalOperatorNode : public BaseOperatorNode,
                                public std::enable_shared_from_this<SortLogicalOperatorNode> {
  public:

    SortLogicalOperatorNode(const Sort& sort_spec);
    const std::string toString() const override;
    OperatorType getOperatorType() const override;

    virtual BaseOperatorNodePtr makeShared() override { return shared_from_this(); };
    virtual bool equals(const BaseOperatorNode& rhs) const override;
  private:
    Sort sort_spec_;
};

} // namespace NES
#endif // SORT_LOGICAL_OPERATOR_NODE_HPP
