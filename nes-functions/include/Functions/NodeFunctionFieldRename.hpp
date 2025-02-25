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
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
namespace NES
{

class NodeFunctionFieldRename;
using NodeFunctionFieldRenamePtr = std::shared_ptr<NodeFunctionFieldRename>;

/**
 * @brief A NodeFunctionFieldRename allows us to rename an attribute value via .as in the query
 */
class NodeFunctionFieldRename : public NodeFunction
{
public:
    /**
     * @brief Create FieldRename Function node
     * @param fieldName : name of the field
     * @param newFieldName : new name of the field
     * @param datatype : the data type
     * @return pointer to the NodeFunctionFieldRename
     */
    static NodeFunctionPtr create(NodeFunctionFieldAccessPtr originalField, std::string newFieldName);

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    bool validateBeforeLowering() const override;


    std::string getNewFieldName() const;

    /**
     * @brief Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this function node.
    * @return NodeFunctionPtr
    */
    NodeFunctionPtr deepCopy() override;

    NodeFunctionFieldAccessPtr getOriginalField() const;

protected:
    explicit NodeFunctionFieldRename(const NodeFunctionFieldRenamePtr other);

    [[nodiscard]] std::string toString() const override;

private:
    NodeFunctionFieldRename(const NodeFunctionFieldAccessPtr& originalField, std::string newFieldName);

    NodeFunctionFieldAccessPtr originalField;
    std::string newFieldName;
};

}
