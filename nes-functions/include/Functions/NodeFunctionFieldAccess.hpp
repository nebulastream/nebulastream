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
namespace NES
{

class NodeFunctionFieldAccess;
using NodeFunctionFieldAccessPtr = std::shared_ptr<NodeFunctionFieldAccess>;

/**
 * @brief A FieldAccessFunction reads a specific field of the current record.
 * It can be created typed or untyped.
 */
class NodeFunctionFieldAccess : public NodeFunction
{
public:
    /**
    * @brief Create typed field read.
    */
    static NodeFunctionPtr create(DataTypePtr stamp, std::string fieldName);

    /**
     * @brief Create untyped field read.
     */
    static NodeFunctionPtr create(std::string fieldName);

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
    * @return NodeFunctionPtr
    */
    NodeFunctionPtr deepCopy() override;

    bool validateBeforeLowering() const override;

protected:
    explicit NodeFunctionFieldAccess(NodeFunctionFieldAccess* other);

    NodeFunctionFieldAccess(DataTypePtr stamp, std::string fieldName);

    std::string toString() const override;
    /**
     * @brief Name of the field want to access.
     */
    std::string fieldName;
};

}
