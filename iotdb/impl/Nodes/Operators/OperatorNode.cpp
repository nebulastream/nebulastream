#include <Nodes/Operators/OperatorNode.hpp>
namespace NES {
OperatorNode::OperatorNode() {}

SchemaPtr OperatorNode::getResultSchema() const {
    // todo we assume that all previous operators have the same output schema otherwise this plan is not valid
    return this->children[0]->as<OperatorNode>()->getResultSchema();
}

size_t OperatorNode::getId() const {
    return id;
}

void OperatorNode::setId(size_t id) {
    OperatorNode::id = id;
}

}

