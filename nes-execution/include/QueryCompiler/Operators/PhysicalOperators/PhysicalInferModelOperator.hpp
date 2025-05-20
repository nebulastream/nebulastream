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

#include <memory>
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <Model.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/**
 * @brief Physical InferModel operator.
 */
class PhysicalInferModelOperator : public PhysicalUnaryOperator
{
public:
    PhysicalInferModelOperator(
        OperatorId id,
        std::shared_ptr<Schema> inputSchema,
        std::shared_ptr<Schema> outputSchema,
        Nebuli::Inference::Model model,
        std::vector<std::shared_ptr<NodeFunction>> inputFields);

    static std::shared_ptr<PhysicalOperator> create(
        OperatorId id,
        const std::shared_ptr<Schema>& inputSchema,
        const std::shared_ptr<Schema>& outputSchema,
        Nebuli::Inference::Model model,
        std::vector<std::shared_ptr<NodeFunction>> inputFields);

    static std::shared_ptr<PhysicalOperator> create(
        const std::shared_ptr<Schema>& inputSchema,
        const std::shared_ptr<Schema>& outputSchema,
        Nebuli::Inference::Model model,
        std::vector<std::shared_ptr<NodeFunction>> inputFields);

    std::shared_ptr<Operator> copy() override;
    const Nebuli::Inference::Model& getModel() const;
    const std::vector<std::shared_ptr<NodeFunction>>& getInputFields() const;
    const std::vector<std::string>& getOutputFields() const;

    std::optional<std::reference_wrapper<const std::string>> registryType() const override
    {
        constexpr static std::string type = "IREE";
        return type;
    }

protected:
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    Nebuli::Inference::Model model;
    const std::vector<std::shared_ptr<NodeFunction>> inputFields;
    const std::vector<std::string> outputFields;
};

}
