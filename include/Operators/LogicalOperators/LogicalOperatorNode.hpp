#ifndef LOGICAL_OPERATOR_NODE_HPP
#define LOGICAL_OPERATOR_NODE_HPP

#include <API/ParameterTypes.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/OperatorNode.hpp>

namespace NES {

class LogicalOperatorNode : public OperatorNode {
  public:
    LogicalOperatorNode(OperatorId id);

    /**
     * @brief Get the First Order Logic formula representation for the logical operator
     * @return and object of type Z3::expr.
     */
    virtual z3::expr getFOL() = 0;
};

}// namespace NES

#endif// LOGICAL_OPERATOR_NODE_HPP
