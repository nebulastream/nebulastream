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
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmetical.hpp>
#include <Functions/NodeFunctionBinary.hpp>
namespace NES
{
/**
 * @brief This node represents a arithmetical function.
 */
class NodeFunctionArithmeticalBinary : public NodeFunctionBinary, public NodeFunctionArithmetical
========
#include <Functions/ArithmeticalFunctions/ArithmeticalFunctionNode.hpp>
#include <Functions/UnaryFunctionNode.hpp>
========
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmetical.hpp>
#include <Functions/NodeFunctionUnary.hpp>
>>>>>>>> a212ae69ac (chore(Functions) Renamed FunctionNode --> NodeFunction):nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionArithmeticalUnary.hpp
namespace NES
{
/**
 * @brief This node represents an arithmetical function.
 */
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp
class ArithmeticalUnaryFunctionNode : public UnaryFunctionNode, public ArithmeticalFunctionNode
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/ArithmeticalFunctions/ArithmeticalUnaryFunctionNode.hpp
========
class NodeFunctionArithmeticalUnary : public NodeFunctionUnary, public ArithmeticalFunctionNode
>>>>>>>> a212ae69ac (chore(Functions) Renamed FunctionNode --> NodeFunction):nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionArithmeticalUnary.hpp
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
    bool validateBeforeLowering() const override;

protected:
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp
    explicit NodeFunctionArithmeticalBinary(DataTypePtr stamp, std::string name);
    explicit NodeFunctionArithmeticalBinary(NodeFunctionArithmeticalBinary* other);
    ~NodeFunctionArithmeticalBinary() noexcept override = default;
========
    explicit ArithmeticalUnaryFunctionNode(DataTypePtr stamp);
    explicit ArithmeticalUnaryFunctionNode(ArithmeticalUnaryFunctionNode* other);
    ~ArithmeticalUnaryFunctionNode() noexcept override = default;
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/ArithmeticalFunctions/ArithmeticalUnaryFunctionNode.hpp
========
    explicit NodeFunctionArithmeticalUnary(DataTypePtr stamp, std::string name);
    explicit NodeFunctionArithmeticalUnary(NodeFunctionArithmeticalUnary* other);
    ~NodeFunctionArithmeticalUnary() noexcept override = default;
>>>>>>>> a212ae69ac (chore(Functions) Renamed FunctionNode --> NodeFunction):nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionArithmeticalUnary.hpp
};

}
