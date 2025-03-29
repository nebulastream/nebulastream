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
#include "Plans/ExecutableOperator.hpp"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include "Identifiers/Identifiers.hpp"
#include "Operators/AbstractOperators/Arity/UnaryOperator.hpp"
#include "Operators/Operator.hpp"

namespace NES::QueryCompilation
{

ExecutableOperator::ExecutableOperator(
    OperatorId id, ExecutablePipelineStagePtr executablePipelineStage, std::vector<std::shared_ptr<OperatorHandler>> operatorHandlers)
    : Operator(id)
    , UnaryOperator(id)
    , executablePipelineStage(std::move(executablePipelineStage))
    , operatorHandlers(std::move(operatorHandlers))
{
}

ExecutableOperator::ExecutableOperator(
    ExecutablePipelineStagePtr executablePipelineStage, std::vector<std::shared_ptr<OperatorHandler>> operatorHandlers)
    : ExecutableOperator(getNextOperatorId(), std::move(executablePipelineStage), std::move(operatorHandlers))
{
}

ExecutablePipelineStagePtr ExecutableOperator::getExecutablePipelineStage()
{
    return executablePipelineStage;
}

std::vector<std::shared_ptr<OperatorHandler>> ExecutableOperator::getOperatorHandlers()
{
    return operatorHandlers;
}

std::string ExecutableOperator::toString() const
{
    return "ExecutableOperator";
}

OperatorPtr ExecutableOperator::clone()
{
    // TODO for now
    return std::dynamic_pointer_cast<Operator>(shared_from_this());
}

}
