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

#include <memory>
#include <sstream>
#include <utility>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalInferModelOperator::PhysicalInferModelOperator(
    OperatorId id,
    std::shared_ptr<Schema> inputSchema,
    std::shared_ptr<Schema> outputSchema,
    Nebuli::Inference::Model model,
    std::vector<std::shared_ptr<NodeFunction>> inputFields)
    : Operator(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
    model(std::move(model)), inputFields(std::move(inputFields))
{
}

std::shared_ptr<PhysicalOperator> PhysicalInferModelOperator::create(
    OperatorId id,
    const std::shared_ptr<Schema>& inputSchema,
    const std::shared_ptr<Schema>& outputSchema,
    Nebuli::Inference::Model model,
    std::vector<std::shared_ptr<NodeFunction>> inputFields)
{
    return std::make_shared<PhysicalInferModelOperator>(id, inputSchema, outputSchema, model, inputFields);
}

std::shared_ptr<PhysicalOperator> PhysicalInferModelOperator::create(
    const std::shared_ptr<Schema>& inputSchema,
    const std::shared_ptr<Schema>& outputSchema,
    Nebuli::Inference::Model model,
    std::vector<std::shared_ptr<NodeFunction>> inputFields)
{
    return create(
        getNextOperatorId(), std::move(inputSchema), std::move(outputSchema),
        std::move(model), std::move(inputFields));
}

std::string PhysicalInferModelOperator::toString() const {
    return "IREE";
}

std::shared_ptr<Operator> PhysicalInferModelOperator::copy()
{
    auto result = create(id, inputSchema, outputSchema, model, inputFields);
    result->addAllProperties(properties);
    return result;
}

const Nebuli::Inference::Model& PhysicalInferModelOperator::getModel() const { return model; }
const std::vector<std::shared_ptr<NodeFunction>>& PhysicalInferModelOperator::getInputFields() const { return inputFields; }

}
