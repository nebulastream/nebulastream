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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>

namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

PhysicalProjectOperator::
PhysicalProjectOperator(OperatorId id, std::vector<ExpressionNodePtr> expressions):
    OperatorNode(id), AbstractProjectionOperator(expressions), PhysicalUnaryOperator(id) {}

PhysicalOperatorPtr PhysicalProjectOperator::create(OperatorId id, std::vector<ExpressionNodePtr> expressions) {
    return std::make_shared<PhysicalProjectOperator>(id, expressions);
}

PhysicalOperatorPtr PhysicalProjectOperator::create(std::vector<ExpressionNodePtr> expressions) {
    return create(UtilityFunctions::getNextOperatorId(), expressions);
}

const std::string PhysicalProjectOperator::toString() const { return "PhysicalProjectOperator"; }

OperatorNodePtr PhysicalProjectOperator::copy() { return create(id, getExpressions()); }

}
}
}