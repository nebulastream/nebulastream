#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALUNARYOPERATORNODE_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALUNARYOPERATORNODE_HPP_

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>

namespace NES {
class LogicalUnaryOperatorNode : public LogicalOperatorNode, public UnaryOperatorNode {

  public:

    LogicalUnaryOperatorNode(OperatorId id);

    /**
    * @brief infers the input and out schema of this operator depending on its child.
    * @throws Exception if the schema could not be infers correctly or if the inferred types are not valid.
    * @return true if schema was correctly inferred
    */
    virtual bool inferSchema() override;

};
}

#endif//NES_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALUNARYOPERATORNODE_HPP_
