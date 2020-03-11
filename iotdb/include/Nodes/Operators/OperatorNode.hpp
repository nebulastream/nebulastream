#ifndef NES_INCLUDE_NODES_OPERATORS_OPERATORNODE_HPP_
#define NES_INCLUDE_NODES_OPERATORS_OPERATORNODE_HPP_

#include <Nodes/Node.hpp>
#include <API/Window/WindowDefinition.hpp>
#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>

namespace NES {

class OperatorNode : public Node {
  public:
    OperatorNode();

    virtual bool equals(const Node&) {};
    virtual bool equal(const NodePtr& rhs) const { return false; };
    virtual bool operator==(const Node&) {};
  private:
    Schema inputSchema;
    Schema outputSchema;
};

typedef std::shared_ptr<OperatorNode> OperatorNodePtr;


}

#endif //NES_INCLUDE_NODES_OPERATORS_OPERATORNODE_HPP_
