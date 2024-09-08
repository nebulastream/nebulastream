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
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionArithmeticalUnary.hpp
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmetical.hpp>
#include <Functions/NodeFunctionUnary.hpp>
namespace NES
{

class NodeFunctionArithmeticalUnary : public NodeFunctionUnary, public NodeFunctionArithmetical
========
#include <Functions/ArithmeticalFunctions/ArithmeticalFunctionNode.hpp>
#include <Functions/BinaryFunctionNode.hpp>
namespace NES
{
/**
 * @brief This node represents a arithmetical function.
 */
class ArithmeticalBinaryFunctionNode : public BinaryFunctionNode, public ArithmeticalFunctionNode
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/ArithmeticalFunctions/ArithmeticalBinaryFunctionNode.hpp
{
public:
    /**
     * @brief Infers the stamp of this arithmetical function node.
     * Currently the type inference is equal for all arithmetical function and expects numerical data types as operands.
     * @param typeInferencePhaseContext
     * @param schema the current schema.
     */
    void inferStamp(SchemaPtr schema) override;
    bool validateBeforeLowering() const override;
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

protected:
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionArithmeticalUnary.hpp
    explicit NodeFunctionArithmeticalUnary(DataTypePtr stamp, std::string name);
    explicit NodeFunctionArithmeticalUnary(NodeFunctionArithmeticalUnary* other);
    ~NodeFunctionArithmeticalUnary() noexcept override = default;
========
    explicit ArithmeticalBinaryFunctionNode(DataTypePtr stamp);
    explicit ArithmeticalBinaryFunctionNode(ArithmeticalBinaryFunctionNode* other);
    ~ArithmeticalBinaryFunctionNode() noexcept override = default;
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/ArithmeticalFunctions/ArithmeticalBinaryFunctionNode.hpp
};

}
