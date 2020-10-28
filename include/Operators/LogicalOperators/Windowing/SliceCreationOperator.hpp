#ifndef NES_INCLUDE_NODES_OPERATORS_SPECIALIZEDWINDOWOPERATORS_SLICECREATIONOPERATOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_SPECIALIZEDWINDOWOPERATORS_SLICECREATIONOPERATOR_HPP_
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>

namespace NES {

/**
 * @brief this class represents the slicing operator for distributed windowing that is deployed on the source nodes and send all sliches to the combiner
 */
class SliceCreationOperator : public WindowOperatorNode {
  public:
    SliceCreationOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id);

    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;
    bool isIdentical(NodePtr rhs) const override;
    virtual bool inferSchema() override;
};

}// namespace NES
#endif//NES_INCLUDE_NODES_OPERATORS_SPECIALIZEDWINDOWOPERATORS_SLICECREATIONOPERATOR_HPP_
