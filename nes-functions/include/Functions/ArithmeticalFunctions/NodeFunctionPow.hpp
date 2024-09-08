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
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionPow.hpp
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
namespace NES
{

class NodeFunctionPow final : public NodeFunctionArithmeticalBinary
{
public:
    explicit NodeFunctionPow(DataTypePtr stamp);
    ~NodeFunctionPow() noexcept override = default;
    static NodeFunctionPtr create(NodeFunctionPtr const& left, NodeFunctionPtr const& right);
========
#include <Functions/ArithmeticalFunctions/ArithmeticalBinaryFunctionNode.hpp>
namespace NES
{
/**
 * @brief This node represents an POWER function.
 */
class PowFunctionNode final : public ArithmeticalBinaryFunctionNode
{
public:
    explicit PowFunctionNode(DataTypePtr stamp);
    ~PowFunctionNode() noexcept override = default;
    /**
     * @brief Create a new POWER function
     */
    static FunctionNodePtr create(FunctionNodePtr const& left, FunctionNodePtr const& right);

>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/ArithmeticalFunctions/PowFunctionNode.hpp
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

    /**
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionPow.hpp
     * @brief Determine returned datatype (-> UInt64/Double/NodeFunction Throw exception for invalid inputs). Override ArithmeticalBinary::inferStamp to increase bounds.
========
     * @brief Determine returned datatype (-> UInt64/Double/ Throw exception for invalid inputs). Override ArithmeticalBinaryFunctionNode::inferStamp to increase bounds.
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/ArithmeticalFunctions/PowFunctionNode.hpp
     * @comment E.g. SQL Server has a very unintuitive behaviour of always returning the datatype of the base (Int/Float). C++ always returns a float. We decide to return a float, except when both base and exponent are an Integer; and we set high bounds as POWER is an exponential function.
     * @param typeInferencePhaseContext
     * @param schema: the current schema.
     */
    void inferStamp(SchemaPtr schema) override;
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionPow.hpp
    NodeFunctionPtr deepCopy() override;

private:
    explicit NodeFunctionPow(NodeFunctionPow* other);
========

    /**
    * @brief Create a deep copy of this function node.
    * @return FunctionNodePtr
    */
    FunctionNodePtr copy() override;

private:
    explicit PowFunctionNode(PowFunctionNode* other);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/ArithmeticalFunctions/PowFunctionNode.hpp
};

}
