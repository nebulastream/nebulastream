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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr PhysicalJoinBuildOperator::create(SchemaPtr inputSchema,
                                                      SchemaPtr outputSchema,
                                                      Join::JoinOperatorHandlerPtr operatorHandler,
                                                      JoinBuildSide buildSide) {
    return create(Util::getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(operatorHandler),
                  buildSide);
}

PhysicalOperatorPtr PhysicalJoinBuildOperator::create(OperatorId id,
                                                      const SchemaPtr& inputSchema,
                                                      const SchemaPtr& outputSchema,
                                                      const Join::JoinOperatorHandlerPtr& operatorHandler,
                                                      JoinBuildSide buildSide) {
    return std::make_shared<PhysicalJoinBuildOperator>(id, inputSchema, outputSchema, operatorHandler, buildSide);
}

PhysicalJoinBuildOperator::PhysicalJoinBuildOperator(OperatorId id,
                                                     SchemaPtr inputSchema,
                                                     SchemaPtr outputSchema,
                                                     Join::JoinOperatorHandlerPtr operatorHandler,
                                                     JoinBuildSide buildSide)
    : OperatorNode(id), PhysicalJoinOperator(std::move(operatorHandler)),
      PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)), joinBuildSide(buildSide){};

std::string PhysicalJoinBuildOperator::toString() const { return "PhysicalJoinBuildOperator"; }

OperatorNodePtr PhysicalJoinBuildOperator::copy() {
    return create(id, inputSchema, outputSchema, operatorHandler, joinBuildSide);
}

JoinBuildSide PhysicalJoinBuildOperator::getBuildSide() { return joinBuildSide; }

}// namespace NES::QueryCompilation::PhysicalOperators