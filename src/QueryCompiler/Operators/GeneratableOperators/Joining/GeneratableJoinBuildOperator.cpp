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

#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableJoinBuildOperator.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

GeneratableOperatorPtr GeneratableJoinBuildOperator::create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Join::JoinOperatorHandlerPtr operatorHandler) {
    return std::make_shared<GeneratableJoinBuildOperator>(
        GeneratableJoinBuildOperator(id, inputSchema, outputSchema, operatorHandler));
}

GeneratableOperatorPtr GeneratableJoinBuildOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, Join::JoinOperatorHandlerPtr operatorHandler) {
    return create(UtilityFunctions::getNextOperatorId(), inputSchema, outputSchema, operatorHandler);
}

GeneratableJoinBuildOperator::GeneratableJoinBuildOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Join::JoinOperatorHandlerPtr operatorHandler)
    : OperatorNode(id), GeneratableJoinOperator(id, inputSchema, outputSchema, operatorHandler) {}

void GeneratableJoinBuildOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {}

const std::string GeneratableJoinBuildOperator::toString() const { return std::string(); }

OperatorNodePtr GeneratableJoinBuildOperator::copy() {
    return create(id, inputSchema, outputSchema, operatorHandler);
}

}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES