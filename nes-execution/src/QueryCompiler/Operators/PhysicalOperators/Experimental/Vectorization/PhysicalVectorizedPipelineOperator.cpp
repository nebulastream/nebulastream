/*
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
#include <QueryCompiler/Operators/PhysicalOperators/Experimental/Vectorization/PhysicalVectorizedPipelineOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators::Experimental {

PhysicalVectorizedPipelineOperator::PhysicalVectorizedPipelineOperator(OperatorId id, const PhysicalUnaryOperatorPtr& vectorizableOperator)
    : OperatorNode(id)
    , PhysicalUnaryOperator(id, vectorizableOperator->getInputSchema(), vectorizableOperator->getOutputSchema())
    , vectorizableOperator(vectorizableOperator)
{

}

PhysicalOperatorPtr PhysicalVectorizedPipelineOperator::create(const PhysicalOperatorPtr& vectorizableOperator) {
    auto physicalUnaryOperator = std::dynamic_pointer_cast<PhysicalUnaryOperator>(vectorizableOperator);
    return std::make_shared<PhysicalVectorizedPipelineOperator>(Util::getNextOperatorId(), physicalUnaryOperator);
}

std::string PhysicalVectorizedPipelineOperator::toString() const {
    return "PhysicalVectorizedPipelineOperator";
}

OperatorNodePtr PhysicalVectorizedPipelineOperator::copy() {
    return std::make_shared<PhysicalVectorizedPipelineOperator>(id, vectorizableOperator);
}

std::vector<NodePtr> PhysicalVectorizedPipelineOperator::getPipelineOperators() {
    std::vector<NodePtr> operators;
    operators.push_back(vectorizableOperator);
    auto op = vectorizableOperator->copy();
    while (!op->getChildren().empty()) {
        auto children = op->getChildren();
        if (children.size() > 1) {
            return {};
        }

        auto child = children.at(0);
        operators.push_back(child);
        op = std::dynamic_pointer_cast<OperatorNode>(child);
    }
    return operators;
}

} // namespace NES::QueryCompilation::PhysicalOperators::Experimental
