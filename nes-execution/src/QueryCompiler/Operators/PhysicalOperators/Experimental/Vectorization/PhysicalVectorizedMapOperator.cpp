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
#include <QueryCompiler/Operators/PhysicalOperators/Experimental/Vectorization/PhysicalVectorizedMapOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators::Experimental {

PhysicalVectorizedMapOperator::PhysicalVectorizedMapOperator(OperatorId id, const std::shared_ptr<PhysicalMapOperator>& physicalMapOperator)
    : OperatorNode(id)
    , PhysicalUnaryOperator(id, physicalMapOperator->getInputSchema(), physicalMapOperator->getOutputSchema())
    , physicalMapOperator(physicalMapOperator)
{

}

PhysicalOperatorPtr PhysicalVectorizedMapOperator::create(const std::shared_ptr<PhysicalMapOperator>& physicalMapOperator) {
    return std::make_shared<PhysicalVectorizedMapOperator>(getNextOperatorId(), physicalMapOperator);
}

std::string PhysicalVectorizedMapOperator::toString() const {
    return "PhysicalVectorizedMapOperator";
}

OperatorNodePtr PhysicalVectorizedMapOperator::copy() {
    return std::make_shared<PhysicalVectorizedMapOperator>(id, physicalMapOperator);
}

const std::shared_ptr<PhysicalMapOperator>& PhysicalVectorizedMapOperator::getPhysicalMapOperator() {
    return physicalMapOperator;
}

} // namespace NES::QueryCompilation::PhysicalOperators::Experimental
