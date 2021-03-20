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
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalOperatorPtr PhysicalSlicePreAggregationOperator::create(Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return create(UtilityFunctions::getNextOperatorId(), windowDefinition);
}

PhysicalOperatorPtr PhysicalSlicePreAggregationOperator::create(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<PhysicalSlicePreAggregationOperator>(id, windowDefinition);
}

PhysicalSlicePreAggregationOperator::PhysicalSlicePreAggregationOperator(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition)
    : OperatorNode(id), AbstractWindowOperator(windowDefinition), PhysicalUnaryOperator(id){};

const std::string PhysicalSlicePreAggregationOperator::toString() const {
    return "PhysicalWindowPreAggregationOperator";
}

OperatorNodePtr PhysicalSlicePreAggregationOperator::copy() {
    return create(id, windowDefinition);
}

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES