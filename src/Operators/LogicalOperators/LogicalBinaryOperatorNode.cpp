#include <Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>

namespace NES {

LogicalBinaryOperatorNode::LogicalBinaryOperatorNode(OperatorId id)
    : OperatorNode(id), LogicalOperatorNode(id), BinaryOperatorNode(id) {}

bool LogicalBinaryOperatorNode::inferSchema() {

    distinctSchemas.clear();
    //Check the number of child operators
    if (children.size() < 2) {
        NES_ERROR("BinaryOperatorNode: this operator should have at least two child operators");
        throw TypeInferenceException("BinaryOperatorNode: this node should have at least two child operators");
    }

    // Infer schema of all child operators
    for (const auto& child : children) {
        if (!child->as<LogicalOperatorNode>()->inferSchema()) {
            NES_ERROR("BinaryOperatorNode: failed inferring the schema of the child operator");
            throw TypeInferenceException("BinaryOperatorNode: failed inferring the schema of the child operator");
        }
    }

    //Identify different type of schemas from children operators
    for (auto& child : children) {
        auto childOutputSchema = child->as<OperatorNode>()->getOutputSchema();
        auto found = std::find_if(distinctSchemas.begin(), distinctSchemas.end(), [&](SchemaPtr distinctSchema) {
          return childOutputSchema->equals(distinctSchema, false);
        });
        if (found == distinctSchemas.end()) {
            distinctSchemas.push_back(childOutputSchema);
        }
    }

    //validate that only two different type of schema were present
    if (distinctSchemas.size() > 2) {
        throw TypeInferenceException("BinaryOperatorNode: Found " + std::to_string(distinctSchemas.size())
                                         + " distinct schemas but expected 2 or less distinct schemas.");
    }

    return true;
}

}// namespace NES