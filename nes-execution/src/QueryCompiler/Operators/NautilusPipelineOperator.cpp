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
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/PhysicalOperatorPipeline.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::QueryCompilation
{

NautilusPipelineOperator::NautilusPipelineOperator(
    OperatorId id,
    std::shared_ptr<PhysicalOperatorPipeline> nautilusPipeline,
    std::vector<std::shared_ptr<OperatorHandler>> operatorHandlers)
    : Operator(id), UnaryOperator(id), operatorHandlers(std::move(operatorHandlers)), nautilusPipeline(std::move(nautilusPipeline))
{
}

std::string NautilusPipelineOperator::toString() const
{
    return "NautilusPipelineOperator";
}

std::shared_ptr<Operator> NautilusPipelineOperator::clone()
{
    auto result = std::make_shared<NautilusPipelineOperator>(getNextOperatorId(), nautilusPipeline, operatorHandlers);
    result->addAllProperties(properties);
    return result;
}

std::shared_ptr<PhysicalOperatorPipeline> NautilusPipelineOperator::getNautilusPipeline()
{
    return nautilusPipeline;
}

std::vector<std::shared_ptr<OperatorHandler>> NautilusPipelineOperator::getOperatorHandlers()
{
    return operatorHandlers;
}

}
