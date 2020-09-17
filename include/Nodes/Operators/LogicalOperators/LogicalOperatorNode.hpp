#ifndef LOGICAL_OPERATOR_NODE_HPP
#define LOGICAL_OPERATOR_NODE_HPP

#include <API/ParameterTypes.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorFactory.hpp>

namespace NES {

class LogicalOperatorNode : public OperatorNode {
  public:
    LogicalOperatorNode();
};

}// namespace NES

#endif// LOGICAL_OPERATOR_NODE_HPP
