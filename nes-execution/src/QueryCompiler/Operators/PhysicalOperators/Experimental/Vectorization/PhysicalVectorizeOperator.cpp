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
#include <QueryCompiler/Operators/PhysicalOperators/Experimental/Vectorization/PhysicalVectorizeOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators::Experimental {

PhysicalVectorizeOperator::PhysicalVectorizeOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema)
    : OperatorNode(id)
    , PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema))
{

}

PhysicalOperatorPtr PhysicalVectorizeOperator::create(OperatorId id, const SchemaPtr& inputSchema, const SchemaPtr& outputSchema) {
    return std::make_shared<PhysicalVectorizeOperator>(id, inputSchema, outputSchema);
}

PhysicalOperatorPtr PhysicalVectorizeOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema) {
    return create(getNextOperatorId(), std::move(inputSchema), std::move(outputSchema));
}

std::string PhysicalVectorizeOperator::toString() const {
    return "PhysicalVectorizeOperator";
}

OperatorNodePtr PhysicalVectorizeOperator::copy() {
    return create(id, inputSchema, outputSchema);
}

} // namespace NES::QueryCompilation::PhysicalOperators::Experimental
