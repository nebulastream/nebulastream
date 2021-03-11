#include <Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>

namespace NES {

LogicalUnaryOperatorNode::LogicalUnaryOperatorNode(OperatorId id)
    : OperatorNode(id), LogicalOperatorNode(id), UnaryOperatorNode(id) {}

bool LogicalUnaryOperatorNode::inferSchema() {

    // We assume that all children operators have the same output schema otherwise this plan is not valid
    for (const auto& child : children) {
        if (!child->as<LogicalOperatorNode>()->inferSchema()) {
            return false;
        }
    }

    if (children.empty()) {
        NES_THROW_RUNTIME_ERROR("UnaryOperatorNode: this node should have at least one child operator");
    }

    auto childSchema = children[0]->as<OperatorNode>()->getOutputSchema();
    for (const auto& child : children) {
        if (!child->as<OperatorNode>()->getOutputSchema()->equals(childSchema)) {
            NES_ERROR("UnaryOperatorNode: infer schema failed. The schema has to be the same across all child operators."
                      " this op schema="
                          << child->as<OperatorNode>()->getOutputSchema()->toString() << " child schema=" << childSchema->toString());
            return false;
        }
    }

    //Reset and reinitialize the input and output schemas
    inputSchema->clear();
    inputSchema = inputSchema->copyFields(childSchema);
    outputSchema->clear();
    outputSchema = outputSchema->copyFields(childSchema);
    return true;
}

}// namespace NES