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
#include <Functions/ArithmeticalFunctions/ArithmeticalFunctionNode.hpp>
#include <Functions/UnaryFunctionNode.hpp>
namespace NES
{
/**
 * @brief This node represents an arithmetical function.
 */
class ArithmeticalUnaryFunctionNode : public UnaryFunctionNode, public ArithmeticalFunctionNode
{
public:
    /**
     * @brief Infers the stamp of this arithmetical function node.
     * Currently the type inference is equal for all arithmetical function and expects numerical data types as operands.
     * @param typeInferencePhaseContext
     * @param schema the current schema.
     */
    void inferStamp(SchemaPtr schema) override;

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

protected:
    explicit ArithmeticalUnaryFunctionNode(DataTypePtr stamp);
    explicit ArithmeticalUnaryFunctionNode(ArithmeticalUnaryFunctionNode* other);
    ~ArithmeticalUnaryFunctionNode() noexcept override = default;
};

} /// namespace NES
