/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_OPERATORS_OPERATOR_NODE_HPP_
#define NES_INCLUDE_OPERATORS_OPERATOR_NODE_HPP_

#include <Nodes/Node.hpp>
#include <Operators/OperatorId.hpp>
#include <any>

namespace NES {

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class OperatorNode : public Node {
  public:
    explicit OperatorNode(OperatorId id);

    ~OperatorNode() noexcept override = default;

    /**
     * @brief gets the operator id.
     * Unique Identifier of the operator within a query.
     * @return u_int64_t
     */
    u_int64_t getId() const;

    /**
     * NOTE: this method is only called from Logical Plan Expansion Rule
     * @brief gets the operator id.
     * Unique Identifier of the operator within a query.
     * @param operator id
     */
    void setId(u_int64_t id);

    /**
     * @brief Create duplicate of this operator by copying its context information and also its parent and child operator set.
     * @return duplicate of this logical operator
     */
    OperatorNodePtr duplicate();

    /**
     * @brief Create a shallow copy of the operator by copying its operator properties but not its children or parent operator tree.
     * @return shallow copy of the operator
     */
    virtual OperatorNodePtr copy() = 0;

    /**
     * @brief detect if this operator is a n-ary operator, i.e., it has multiple parent or children.
     * @return true if n-ary else false;
     */
    bool hasMultipleChildrenOrParents();

    /**
    * @brief return if the operator has multiple children
    * @return bool
    */
    bool hasMultipleChildren();

    /**
    * @brief return if the operator has multiple children
    * @return bool
    */
    bool hasMultipleParents();

    /**
     * @brief method to add a child to this node
     * @param newNode
     * @return bool indicating success
     */
    bool addChild(NodePtr newNode) override;

    /**
    * @brief method to add a parent to this node
    * @param newNode
    * @return bool indicating success
    */
    bool addParent(NodePtr newNode) override;

    /**
     * @brief Get the operator with input operator id
     * @param operatorId : the if of the operator to find
     * @return nullptr if not found else the operator node
     */
    NodePtr getChildWithOperatorId(uint64_t operatorId);

    /**
     * @brief Method to get the output schema of the operator
     * @return output schema
     */
    virtual SchemaPtr getOutputSchema() const = 0;

    /**
     * @brief Method to set the output schema
     * @param outputSchema
     */
    virtual void setOutputSchema(SchemaPtr outputSchema) = 0;

    /**
     * @brief This methods return if the operator is a binary operator, i.e., as two input schemas
     * @return bool
     */
    virtual bool isBinaryOperator() const = 0;

    /**
    * @brief This methods return if the operator is a unary operator, i.e., as oneinput schemas
    * @return bool
     */
    virtual bool isUnaryOperator() const = 0;

    /**
    * @brief This methods return if the operator is an exchange operator, i.e., it has potentially multiple output schemas
    * @return bool
    */
    virtual bool isExchangeOperator() const = 0;

    /**
     * @brief Add a new property string to the stored properties map
     * @param key key of the new property
     * @param value value of the new property
     */
    void addProperty(const std::string& key, const std::any& value);

    /**
     * @brief Get a the value of a property
     * @param key key of the value to retrieve
     * @return value of the property with the given key
     */
    std::any getProperty(const std::string& key);

    /**
     * @brief Remove a property string from the stored properties map
     * @param key key of the property to remove
     */
    void removeProperty(const std::string& key);

    /**
     * Check if the given property exists in the current operator
     * @param key key of the property to check
     * @return true if property exists
     */
    bool checkIfPropertyExist(const std::string& key);

  protected:
    /**
     * @brief get duplicate of the input operator and all its ancestors
     * @param operatorNode: the input operator
     * @return duplicate of the input operator
     */
    OperatorNodePtr getDuplicateOfParent(const OperatorNodePtr& operatorNode);

    /**
     * @brief get duplicate of the input operator and all its children
     * @param operatorNode: the input operator
     * @return duplicate of the input operator
     */
    OperatorNodePtr getDuplicateOfChild(const OperatorNodePtr& operatorNode);

    /**
     * @brief Unique Identifier of the operator within a query.
     */
    u_int64_t id;

    /*
     * @brief Map of properties of the current node
     */
    std::map<std::string, std::any> properties;
};

}// namespace NES

#endif// NES_INCLUDE_OPERATORS_OPERATOR_NODE_HPP_
