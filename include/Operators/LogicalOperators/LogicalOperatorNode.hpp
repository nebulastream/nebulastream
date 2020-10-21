#ifndef LOGICAL_OPERATOR_NODE_HPP
#define LOGICAL_OPERATOR_NODE_HPP

#include <API/ParameterTypes.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/OperatorNode.hpp>

namespace NES {

class LogicalOperatorNode : public OperatorNode {
  public:
    LogicalOperatorNode();
};

}// namespace NES

#endif// LOGICAL_OPERATOR_NODE_HPP
