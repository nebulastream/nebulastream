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
#include <QueryCompiler/Operators/PhysicalOperators/Experimental/Vectorization/PhysicalVectorizedNonKeyedPreAggregationOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators::Experimental {

PhysicalVectorizedNonKeyedPreAggregationOperator::PhysicalVectorizedNonKeyedPreAggregationOperator(OperatorId id, const std::shared_ptr<PhysicalNonKeyedThreadLocalPreAggregationOperator>& physicalOperator)
    : OperatorNode(id)
    , PhysicalUnaryOperator(id, physicalOperator->getInputSchema(), physicalOperator->getOutputSchema())
    , physicalOperator(physicalOperator)
{

}

PhysicalOperatorPtr PhysicalVectorizedNonKeyedPreAggregationOperator::create(const std::shared_ptr<PhysicalNonKeyedThreadLocalPreAggregationOperator>& physicalOperator) {
    return std::make_shared<PhysicalVectorizedNonKeyedPreAggregationOperator>(Util::getNextOperatorId(), physicalOperator);
}

std::string PhysicalVectorizedNonKeyedPreAggregationOperator::toString() const {
    return "PhysicalVectorizedNonKeyedPreAggregationOperator";
}

OperatorNodePtr PhysicalVectorizedNonKeyedPreAggregationOperator::copy() {
    return std::make_shared<PhysicalVectorizedNonKeyedPreAggregationOperator>(id, physicalOperator);
}

const std::shared_ptr<PhysicalNonKeyedThreadLocalPreAggregationOperator>& PhysicalVectorizedNonKeyedPreAggregationOperator::getPhysicalNonKeyedPreAggregationOperator() {
    return physicalOperator;
}

} // namespace NES::QueryCompilation::PhysicalOperators::Experimental
