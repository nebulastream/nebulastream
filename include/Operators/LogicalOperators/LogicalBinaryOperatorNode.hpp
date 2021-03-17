#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALBINARYOPERATORNODE_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALBINARYOPERATORNODE_HPP_

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/AbstractOperators/Arity/BinaryOperatorNode.hpp>

namespace NES {
class LogicalBinaryOperatorNode : public LogicalOperatorNode, public BinaryOperatorNode {
  public:
    LogicalBinaryOperatorNode(OperatorId id);

    /**
    * @brief infers the input and out schema of this operator depending on its child.
    * @throws Exception if the schema could not be infers correctly or if the inferred types are not valid.
    * @return true if schema was correctly inferred
    */
    virtual bool inferSchema() override;

    /**
     * @brief Get all left input operators.
     * @return std::vector<OperatorNodePtr>
     */
    std::vector<OperatorNodePtr> getLeftOperators();

    /**
    * @brief Get all right input operators.
    * @return std::vector<OperatorNodePtr>
    */
    std::vector<OperatorNodePtr> getRightOperators();

  private:
    std::vector<OperatorNodePtr> getOperatorsBySchema(SchemaPtr schema);

};
}

#endif//NES_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALBINARYOPERATORNODE_HPP_
