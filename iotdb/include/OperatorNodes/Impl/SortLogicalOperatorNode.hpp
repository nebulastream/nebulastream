#ifndef SORT_LOGICAL_OPERATOR_NODE_HPP
#define SORT_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <OperatorNodes/Node.hpp>
#include <API/ParameterTypes.hpp>

namespace NES {

class SortLogicalOperatorNode : public Node,
                                public std::enable_shared_from_this<SortLogicalOperatorNode> {
  public:

    SortLogicalOperatorNode(const Sort& sort_spec);
    const std::string toString() const override;

    virtual NodePtr makeShared() override { return shared_from_this(); };
    virtual bool equal(const Node& rhs) const override;
  private:
    Sort sort_spec_;
};

} // namespace NES
#endif // SORT_LOGICAL_OPERATOR_NODE_HPP
