/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Arity/BinaryOperatorNode.hpp>

namespace NES {

BinaryOperatorNode::BinaryOperatorNode(OperatorId id)
    : LogicalOperatorNode(id), leftInputSchema(Schema::create()), rightInputSchema(Schema::create()),
      outputSchema(Schema::create()) {
    //nop
}

bool BinaryOperatorNode::isBinaryOperator() const { return true; }

bool BinaryOperatorNode::isUnaryOperator() const { return false; }

bool BinaryOperatorNode::isExchangeOperator() const { return false; }

void BinaryOperatorNode::setLeftInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->leftInputSchema = std::move(inputSchema);
    }
}

void BinaryOperatorNode::setRightInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->rightInputSchema = std::move(inputSchema);
    }
}
void BinaryOperatorNode::setOutputSchema(SchemaPtr outputSchema) {
    if (outputSchema) {
        this->outputSchema = std::move(outputSchema);
    }
}
SchemaPtr BinaryOperatorNode::getLeftInputSchema() const { return leftInputSchema; }

SchemaPtr BinaryOperatorNode::getRightInputSchema() const { return rightInputSchema; }

SchemaPtr BinaryOperatorNode::getOutputSchema() const { return outputSchema; }

bool BinaryOperatorNode::inferSchema() {
    // We assume that all children operators have the same output schema otherwise this plan is not valid
    for (const auto& child : children) {
        if (!child->as<OperatorNode>()->inferSchema()) {
            return false;
        }
    }
    if (children.empty()) {
        NES_THROW_RUNTIME_ERROR("BinaryOperatorNode: this node should have at least one child operator");
    }

    if(children.size() >= 2)
    {
        //TODO: think about checking also if all left/right have the same schema
        //find first left
        for(auto& leftOp : children)
        {
            if(leftOp->as<OperatorNode>()->getIsLeftOperator())
            {
                leftInputSchema = leftOp->as<OperatorNode>()->getOutputSchema();
            }
        }

        //find first right
        for(auto& rightOp : children)
        {
            if(!rightOp->as<OperatorNode>()->getIsLeftOperator())
            {
                rightInputSchema = rightOp->as<OperatorNode>()->getOutputSchema();
            }
        }
        NES_ASSERT(leftInputSchema, "no left input for join");
        NES_ASSERT(rightInputSchema, "no left input for join");
    }
    else if(children.size() == 1)
    {
        //special case of self join
        leftInputSchema = children[0]->as<OperatorNode>()->getOutputSchema();
        rightInputSchema = children[0]->as<OperatorNode>()->getOutputSchema();
    }

    NES_DEBUG("Binary infer left schema=" << leftInputSchema->toString() << " right schema=" << rightInputSchema->toString());

    //TODO: this is only temporary and we need a solution to detect which schema is the final schema
    outputSchema = leftInputSchema;
    return true;
}

}// namespace NES
