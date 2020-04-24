#include <Nodes/Operators/OperatorNode.hpp>
#include <API/Schema.hpp>
namespace NES {
/**
 * @brief We initialize the input and output schemas with empty schemas.
 */
OperatorNode::OperatorNode() : inputSchema(Schema::create()), outputSchema(Schema::create()) {}

SchemaPtr OperatorNode::getInputSchema() const {
    return inputSchema;
}

SchemaPtr OperatorNode::getOutputSchema() const {
    return outputSchema;
}

bool OperatorNode::inferSchema() {
    // We assume that all children operators have the same output schema otherwise this plan is not valid
    for(const auto& child: children){
        child->as<OperatorNode>()->inferSchema();
    }
    auto childSchema = children[0]->as<OperatorNode>()->getOutputSchema();
    for(const auto& child: children){
        if(!child->as<OperatorNode>()->getOutputSchema()->equals(childSchema)){
            NES_THROW_RUNTIME_ERROR("OperatorNode: infer schema failed. The schema has to be the same across all child operators.");
            return false;
        }
    }
    inputSchema = childSchema->copy();
    outputSchema = childSchema->copy();
    return true;
}

size_t OperatorNode::getId() const {
    return id;
}

void OperatorNode::setId(size_t id) {
    OperatorNode::id = id;
}

}

