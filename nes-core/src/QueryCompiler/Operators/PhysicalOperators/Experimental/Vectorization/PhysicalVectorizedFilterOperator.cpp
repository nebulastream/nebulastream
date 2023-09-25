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
#include <QueryCompiler/Operators/PhysicalOperators/Experimental/Vectorization/PhysicalVectorizedFilterOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators::Experimental {

PhysicalVectorizedFilterOperator::PhysicalVectorizedFilterOperator(OperatorId id, const std::shared_ptr<PhysicalFilterOperator>& physicalFilterOperator)
    : OperatorNode(id)
    , PhysicalUnaryOperator(id, physicalFilterOperator->getInputSchema(), physicalFilterOperator->getOutputSchema())
    , physicalFilterOperator(physicalFilterOperator)
{

}

PhysicalOperatorPtr PhysicalVectorizedFilterOperator::create(const std::shared_ptr<PhysicalFilterOperator>& physicalFilterOperator) {
    return std::make_shared<PhysicalVectorizedFilterOperator>(Util::getNextOperatorId(), physicalFilterOperator);
}

std::string PhysicalVectorizedFilterOperator::toString() const {
    return "PhysicalVectorizedFilterOperator";
}

OperatorNodePtr PhysicalVectorizedFilterOperator::copy() {
    return std::make_shared<PhysicalVectorizedFilterOperator>(id, physicalFilterOperator);
}

const std::shared_ptr<PhysicalFilterOperator>& PhysicalVectorizedFilterOperator::getPhysicalFilterOperator() {
    return physicalFilterOperator;
}

} // namespace NES::QueryCompilation::PhysicalOperators::Experimental
