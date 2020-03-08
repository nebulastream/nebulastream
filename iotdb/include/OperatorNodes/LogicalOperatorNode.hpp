#ifndef LOGICAL_OPERATOR_NODE_HPP
#define LOGICAL_OPERATOR_NODE_HPP

#include <OperatorNodes/Node.hpp>


namespace NES {

class LogicalOperatorNode : public Node {
public:
    LogicalOperatorNode();


    virtual bool equals(const Node& ) {};
    virtual bool operator==(const Node& ) {};
private:
    Schema inputSchema;
    Schema outputSchema;
};

typedef std::shared_ptr<LogicalOperatorNode> LogicalOperatorNodePtr;

// #include <OperatorNodes/Impl/FilterLogicalOperatorNode.hpp>
// #include <OperatorNodes/Impl/SourceLogicalOperatorNode.hpp>
}

#endif  // LOGICAL_OPERATOR_NODE_HPP
