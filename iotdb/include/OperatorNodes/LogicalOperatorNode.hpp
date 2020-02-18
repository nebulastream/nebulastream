#ifndef LOGICAL_OPERATOR_NODE_HPP
#define LOGICAL_OPERATOR_NODE_HPP

#include <OperatorNodes/BaseOperatorNode.hpp>


namespace NES {

class LogicalOperatorNode : public BaseOperatorNode {
public:
    LogicalOperatorNode() = default;



private:
    Schema inputSchema;
    Schema outputSchema;
};

typedef std::shared_ptr<LogicalOperatorNode> LogicalOperatorNodePtr;

// #include <OperatorNodes/Impl/FilterLogicalOperatorNode.hpp>
// #include <OperatorNodes/Impl/SourceLogicalOperatorNode.hpp>
}

#endif  // LOGICAL_OPERATOR_NODE_HPP
