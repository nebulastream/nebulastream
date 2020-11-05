#ifndef NES_WATERMARKASSIGNERLOGICALOPERATORNODE_HPP
#define NES_WATERMARKASSIGNERLOGICALOPERATORNODE_HPP

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>

namespace NES {

class WatermarkAssignerLogicalOperatorNode : public LogicalOperatorNode {
  public:
    WatermarkAssignerLogicalOperatorNode(const Windowing::WatermarkStrategyPtr watermarkStrategy, OperatorId id);

    Windowing::WatermarkStrategyPtr getWatermarkStrategy();

    /**
     * @brief infers the input and out schema of this operator depending on its child.
     * @throws Exception the predicate expression has to return a boolean.
     * @return true if schema was correctly inferred
     */
    bool inferSchema() override;

    bool equal(const NodePtr rhs) const override;

    bool isIdentical(NodePtr rhs) const override;

    const std::string toString() const override;

    OperatorNodePtr copy() override;

    z3::expr getZ3Expression(z3::context& context) override;

  private:
    Windowing::WatermarkStrategyPtr watermarkStrategy;
};

typedef std::shared_ptr<WatermarkAssignerLogicalOperatorNode> WatermarkAssignerLogicalOperatorNodePtr;

}// namespace NES

#endif//NES_WATERMARKASSIGNERLOGICALOPERATORNODE_HPP

