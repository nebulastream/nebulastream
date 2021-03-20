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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalOperatorPtr PhysicalJoinSinkOperator::create(Join::LogicalJoinDefinitionPtr joinDefinition) {
    return create(UtilityFunctions::getNextOperatorId(), joinDefinition);
}

PhysicalOperatorPtr PhysicalJoinSinkOperator::create(OperatorId id, Join::LogicalJoinDefinitionPtr joinDefinition) {
    return std::make_shared<PhysicalJoinSinkOperator>(id, joinDefinition);
}

PhysicalJoinSinkOperator::PhysicalJoinSinkOperator(OperatorId id, Join::LogicalJoinDefinitionPtr joinDefinition)
    : OperatorNode(id), PhysicalBinaryOperator(id), AbstractJoinOperator(joinDefinition) {};

const std::string PhysicalJoinSinkOperator::toString() const {
    return "PhysicalJoinSinkOperator";
}

OperatorNodePtr PhysicalJoinSinkOperator::copy() {
    return create(id, joinDefinition);
}

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES