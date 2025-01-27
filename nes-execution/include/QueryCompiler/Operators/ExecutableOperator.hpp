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
#pragma once
#include <vector>
#include <Operators/AbstractOperators/Arity/UnaryOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <ExecutablePipelineStage.hpp>

namespace NES::QueryCompilation
{

/**
 * @brief A executable operator, represents an executable version of one or more operators in a query plan.
 * It is currently used to represent compiled executable pipeline stages in a query plan.
 * Based on this query plan we then create the executable query plan.
 */
class ExecutableOperator : public UnaryOperator
{
public:
    /**
     * @brief Creates a new executable operator, which captures a pipeline stage and a set of operator handlers.
     * @param executablePipelineStage the executable pipeline stage
     * @param operatorHandlers a list of operator handlers
     * @return std::shared_ptr<Operator>
     */
    static std::shared_ptr<Operator> create(
        std::unique_ptr<Runtime::Execution::ExecutablePipelineStage> executablePipelineStage,
        std::vector<std::shared_ptr<Runtime::Execution::OperatorHandler>> operatorHandlers);

    /**
     * @brief Gets the executable pipeline stage.
     * @return ExecutablePipelineStagePtr
     */
    const Runtime::Execution::ExecutablePipelineStage& getStage() const;

    std::unique_ptr<Runtime::Execution::ExecutablePipelineStage> takeStage();

    /**
     * @brief Gets the operator handlers, which capture specific operator state.
     * @return std::vector<std::shared_ptr<Runtime::Execution::OperatorHandler>>
     */
    std::vector<std::shared_ptr<Runtime::Execution::OperatorHandler>> getOperatorHandlers();
    std::shared_ptr<Operator> copy() override;

protected:
    std::string toString() const override;

private:
    ExecutableOperator(
        OperatorId id,
        std::unique_ptr<Runtime::Execution::ExecutablePipelineStage> executablePipelineStage,
        std::vector<std::shared_ptr<Runtime::Execution::OperatorHandler>> operatorHandlers);
    std::unique_ptr<Runtime::Execution::ExecutablePipelineStage> executablePipelineStage;
    std::vector<std::shared_ptr<Runtime::Execution::OperatorHandler>> operatorHandlers;
};

}
