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
#include <string>
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{


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
    static std::shared_ptr<NodeFunction> create(std::shared_ptr<DataType> stamp, std::string fieldName);

    /**
     * @brief Create untyped field read.
     */
    static std::shared_ptr<NodeFunction> create(std::string fieldName);

    bool equal(const std::shared_ptr<Node>& rhs) const override;

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
    void inferStamp(const Schema& schema) override;

    /**
    * @brief Create a deep copy of this function node.
    * @return std::shared_ptr<NodeFunction>
    */
    std::shared_ptr<NodeFunction> deepCopy() override;

    bool validateBeforeLowering() const override;

protected:
    explicit NodeFunctionFieldAccess(NodeFunctionFieldAccess* other);

    NodeFunctionFieldAccess(std::shared_ptr<DataType> stamp, std::string fieldName);

    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;
    /**
     * @brief Name of the field want to access.
     */
    std::string fieldName;
};

}
