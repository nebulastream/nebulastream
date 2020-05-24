#include <API/Schema.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <utility>
namespace NES {
/**
 * @brief We initialize the input and output schemas with empty schemas.
 */
OperatorNode::OperatorNode() : inputSchema(Schema::create()), outputSchema(Schema::create()), id(0) {}

SchemaPtr OperatorNode::getInputSchema() const {
    return inputSchema;
}

SchemaPtr OperatorNode::getOutputSchema() const {
    return outputSchema;
}

bool OperatorNode::inferSchema() {
    // We assume that all children operators have the same output schema otherwise this plan is not valid
    for (const auto& child : children) {
        child->as<OperatorNode>()->inferSchema();
    }
    auto childSchema = children[0]->as<OperatorNode>()->getOutputSchema();
    for (const auto& child : children) {
        if (!child->as<OperatorNode>()->getOutputSchema()->equals(childSchema)) {
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

void OperatorNode::setInputSchema(SchemaPtr inputSchema) {
    this->inputSchema = std::move(inputSchema);
}

void OperatorNode::setOutputSchema(SchemaPtr outputSchema) {
    this->outputSchema = std::move(outputSchema);
}

}// namespace NES
