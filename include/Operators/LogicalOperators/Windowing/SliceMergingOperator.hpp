#ifndef NES_INCLUDE_NODES_OPERATORS_SPECIALIZEDWINDOWOPERATORS_SLICEMERGINGOPERATOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_SPECIALIZEDWINDOWOPERATORS_SLICEMERGINGOPERATOR_HPP_
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <z3++.h>

namespace NES {

/**
 * @brief this class represents the intermediate merger for distributed windowing that is deployed on the worker
 * nodes between the slicer and combiner to do intermediate merging
 */
class SliceMergingOperator : public WindowOperatorNode {
  public:
    SliceMergingOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id);

    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;
    bool isIdentical(NodePtr rhs) const override;
    virtual bool inferSchema() override;
    z3::expr getZ3Expression(z3::context& context) override;
};

}// namespace NES
#endif//NES_INCLUDE_NODES_OPERATORS_SPECIALIZEDWINDOWOPERATORS_SLICEMERGINGOPERATOR_HPP_
