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
#include <Functions/ArithmeticalFunctions/ArithmeticalUnaryFunctionNode.hpp>
namespace NES
{
/**
 * @brief This node represents an FLOOR (floor of) function.
 */
class FloorFunctionNode final : public ArithmeticalUnaryFunctionNode
{
public:
    explicit FloorFunctionNode(DataTypePtr stamp);
    ~FloorFunctionNode() noexcept override = default;
    /**
     * @brief Create a new FLOOR function
     */
    [[nodiscard]] static FunctionNodePtr create(FunctionNodePtr const& child);
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(SchemaPtr schema) override;
bool validateBeforeLowering() const override;
    FunctionNodePtr deepCopy() override;

private:
    explicit FloorFunctionNode(FloorFunctionNode* other);
};

} /// namespace NES
