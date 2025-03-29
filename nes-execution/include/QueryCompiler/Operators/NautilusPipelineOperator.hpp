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
#include <Operators/AbstractOperators/Arity/UnaryOperator.hpp>
#include <Plans/PhysicalOperatorPipeline.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::QueryCompilation
{

class NautilusPipelineOperator final : public UnaryOperator
{
public:
    std::shared_ptr<Operator> clone() override;
    std::shared_ptr<PhysicalOperatorPipeline> getNautilusPipeline();

    std::vector<std::shared_ptr<OperatorHandler>> getOperatorHandlers();

    NautilusPipelineOperator(
        OperatorId id,
        std::shared_ptr<PhysicalOperatorPipeline> nautilusPipeline,
        std::vector<std::shared_ptr<OperatorHandler>> operatorHandlers);

protected:
    std::string toString() const override;

private:
    std::vector<std::shared_ptr<OperatorHandler>> operatorHandlers;
    std::shared_ptr<PhysicalOperatorPipeline> nautilusPipeline;
};

}
