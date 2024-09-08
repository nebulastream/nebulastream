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
#include <Functions/BinaryFunctionNode.hpp>
#include <Functions/FieldAccessFunctionNode.hpp>
namespace NES
{

class FieldAssignmentFunctionNode;
using FieldAssignmentFunctionNodePtr = std::shared_ptr<FieldAssignmentFunctionNode>;
/**
 * @brief A FieldAssignmentFunction represents the assignment of an function result to a specific field.
 */
class FieldAssignmentFunctionNode : public BinaryFunctionNode
{
public:
    explicit FieldAssignmentFunctionNode(DataTypePtr stamp);

    /**
     * @brief Create untyped field read.
     */
    static FieldAssignmentFunctionNodePtr
    create(const FieldAccessFunctionNodePtr& fieldAccess, const FunctionNodePtr& functionNodePtr);

    [[nodiscard]] std::string toString() const override;
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;

    /**
     * @brief return the field to which a new value is assigned.
     * @return FieldAccessFunctionNodePtr
     */
    FieldAccessFunctionNodePtr getField() const;

    /**
     * @brief returns the functions, which calculates the new value.
     * @return FunctionNodePtr
     */
    FunctionNodePtr getAssignment() const;

    /**
     * @brief Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this function node.
    * @return FunctionNodePtr
    */
    FunctionNodePtr copy() override;

protected:
    explicit FieldAssignmentFunctionNode(FieldAssignmentFunctionNode* other);
};
} /// namespace NES
