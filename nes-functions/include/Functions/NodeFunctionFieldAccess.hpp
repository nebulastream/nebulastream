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
<<<<<<<< HEAD:nes-functions/include/Functions/NodeFunctionFieldAccess.hpp
#include <Functions/NodeFunction.hpp>
namespace NES
{

class NodeFunctionFieldAccess;
using NodeFunctionFieldAccessPtr = std::shared_ptr<NodeFunctionFieldAccess>;
========
#include <Functions/FunctionNode.hpp>
namespace NES
{

class FieldAccessFunctionNode;
using FieldAccessFunctionNodePtr = std::shared_ptr<FieldAccessFunctionNode>;
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/FieldAccessFunctionNode.hpp

/**
 * @brief A FieldAccessFunction reads a specific field of the current record.
 * It can be created typed or untyped.
 */
<<<<<<<< HEAD:nes-functions/include/Functions/NodeFunctionFieldAccess.hpp
class NodeFunctionFieldAccess : public NodeFunction
========
class FieldAccessFunctionNode : public FunctionNode
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/FieldAccessFunctionNode.hpp
{
public:
    /**
    * @brief Create typed field read.
    */
<<<<<<<< HEAD:nes-functions/include/Functions/NodeFunctionFieldAccess.hpp
    static NodeFunctionPtr create(DataTypePtr stamp, std::string fieldName);
========
    static FunctionNodePtr create(DataTypePtr stamp, std::string fieldName);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/FieldAccessFunctionNode.hpp

    /**
     * @brief Create untyped field read.
     */
<<<<<<<< HEAD:nes-functions/include/Functions/NodeFunctionFieldAccess.hpp
    static NodeFunctionPtr create(std::string fieldName);
========
    static FunctionNodePtr create(std::string fieldName);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/FieldAccessFunctionNode.hpp

    std::string toString() const override;
    bool equal(NodePtr const& rhs) const override;

    /**
     * @brief Get field name
     * @return field name
     */
    std::string getFieldName() const;

    /**
     * @brief Updated field name
     * @param fieldName : the new name of the field
     */
    void updateFieldName(std::string fieldName);

    /**
     * @brief Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this function node.
<<<<<<<< HEAD:nes-functions/include/Functions/NodeFunctionFieldAccess.hpp
    * @return NodeFunctionPtr
    */
    NodeFunctionPtr deepCopy() override;

    bool validateBeforeLowering() const override;

protected:
    explicit NodeFunctionFieldAccess(NodeFunctionFieldAccess* other);

    NodeFunctionFieldAccess(DataTypePtr stamp, std::string fieldName);
========
    * @return FunctionNodePtr
    */
    FunctionNodePtr copy() override;

protected:
    explicit FieldAccessFunctionNode(FieldAccessFunctionNode* other);

    FieldAccessFunctionNode(DataTypePtr stamp, std::string fieldName);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/FieldAccessFunctionNode.hpp
    /**
     * @brief Name of the field want to access.
     */
    std::string fieldName;
};

} /// namespace NES
