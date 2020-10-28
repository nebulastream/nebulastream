#include <API/Schema.hpp>
#include <Operators/OperatorNode.hpp>
#include <Util/UtilityFunctions.hpp>
#include <algorithm>
#include <utility>

namespace NES {
/**
 * @brief We initialize the input and output schemas with empty schemas.
 */
OperatorNode::OperatorNode() : id(UtilityFunctions::getNextOperatorId()), inputSchema(Schema::create()), outputSchema(Schema::create()) {}

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
    if (children.empty()) {
        NES_THROW_RUNTIME_ERROR("OperatorNode: this node should have at least one child operator");
    }
    auto childSchema = children[0]->as<OperatorNode>()->getOutputSchema();
    for (const auto& child : children) {
        if (!child->as<OperatorNode>()->getOutputSchema()->equals(childSchema)) {
            NES_THROW_RUNTIME_ERROR("OperatorNode: infer schema failed. The schema has to be the same across all child operators.");
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

bool OperatorNode::isNAryOperator() {
    //has multiple child operator
    bool hasMultipleChildren = (!getChildren().empty()) && getChildren().size() > 1;
    //has multiple parent operator
    bool hasMultipleParent = (!getParents().empty()) && getParents().size() > 1;
    NES_DEBUG("OperatorNode: has multiple children " << hasMultipleChildren << " or has multiple parent " << hasMultipleParent);
    return hasMultipleChildren || hasMultipleParent;
}

OperatorNodePtr OperatorNode::duplicate() {
    NES_INFO("OperatorNode: Create copy of the operator");
    const OperatorNodePtr copyOperator = copy();

    NES_DEBUG("OperatorNode: copy all parents");
    for (auto& parent : getParents()) {
        if (!copyOperator->addParent(getDuplicateOfParent(parent->as<OperatorNode>()))) {
            NES_THROW_RUNTIME_ERROR("OperatorNode: Unable to add parent to copy");
        }
    }

    NES_DEBUG("OperatorNode: copy all children");
    for (auto& child : getChildren()) {
        if (!copyOperator->addChild(getDuplicateOfChild(child->as<OperatorNode>()->duplicate()))) {
            NES_THROW_RUNTIME_ERROR("OperatorNode: Unable to add child to copy");
        }
    }
    return copyOperator;
}

OperatorNodePtr OperatorNode::getDuplicateOfParent(OperatorNodePtr operatorNode) {
    NES_DEBUG("OperatorNode: create copy of the input operator");
    const OperatorNodePtr& copyOfOperator = operatorNode->copy();
    if (operatorNode->getParents().empty()) {
        NES_TRACE("OperatorNode: No ancestor of the input node. Returning the copy of the input operator");
        return copyOfOperator;
    }

    NES_TRACE("OperatorNode: For all parents get copy of the ancestor and add as parent to the copy of the input operator");
    for (auto& parent : operatorNode->getParents()) {
        copyOfOperator->addParent(getDuplicateOfParent(parent->as<OperatorNode>()));
    }
    NES_TRACE("OperatorNode: return copy of the input operator");
    return copyOfOperator;
}

OperatorNodePtr OperatorNode::getDuplicateOfChild(OperatorNodePtr operatorNode) {
    NES_DEBUG("OperatorNode: create copy of the input operator");
    OperatorNodePtr copyOfOperator = operatorNode->copy();
    if (operatorNode->getChildren().empty()) {
        NES_TRACE("OperatorNode: No children of the input node. Returning the copy of the input operator");
        return copyOfOperator;
    }

    NES_TRACE("OperatorNode: For all children get copy of their children and add as child to the copy of the input operator");
    for (auto& child : operatorNode->getChildren()) {
        copyOfOperator->addChild(getDuplicateOfParent(child->as<OperatorNode>()));
    }
    NES_TRACE("OperatorNode: return copy of the input operator");
    return copyOfOperator;
}

bool OperatorNode::addChild(NodePtr newNode) {

    if (newNode->as<OperatorNode>()->getId() == id) {
        NES_ERROR("OperatorNode: can not add self as child to itself");
        return false;
    }

    std::vector<NodePtr> currentChildren = getChildren();
    auto found = std::find_if(currentChildren.begin(), currentChildren.end(), [&](NodePtr child) {
        return child->as<OperatorNode>()->getId() == newNode->as<OperatorNode>()->getId();
    });

    if (found == currentChildren.end()) {
        NES_DEBUG("OperatorNode: Adding node to the children.");
        children.push_back(newNode);
        newNode->addParent(shared_from_this());
        return true;
    }
    NES_DEBUG("OperatorNode: the node is already part of its children so skip add child operation.");
    return false;
}

bool OperatorNode::addParent(NodePtr newNode) {

    if (newNode->as<OperatorNode>()->getId() == id) {
        NES_ERROR("OperatorNode: can not add self as parent to itself");
        return false;
    }

    std::vector<NodePtr> currentParents = getParents();
    auto found = std::find_if(currentParents.begin(), currentParents.end(), [&](NodePtr child) {
        return child->as<OperatorNode>()->getId() == newNode->as<OperatorNode>()->getId();
    });

    if (found == currentParents.end()) {
        NES_DEBUG("OperatorNode: Adding node to the Parents.");
        parents.push_back(newNode);
        newNode->addChild(shared_from_this());
        return true;
    }
    NES_DEBUG("OperatorNode: the node is already part of its parent so skip add parent operation.");
    return false;
}

NodePtr OperatorNode::getChildWithOperatorId(uint64_t operatorId) {

    if (id == operatorId) {
        return shared_from_this();
    }
    for (auto& child : children) {
        auto found = child->as<OperatorNode>()->getChildWithOperatorId(operatorId);
        if (found) {
            return found;
        }
    }
    return nullptr;
}

}// namespace NES
