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

#ifndef NES_INCLUDE_OPERATORS_OPERATORNODE_HPP_
#define NES_INCLUDE_OPERATORS_OPERATORNODE_HPP_

#include <Nodes/Node.hpp>
#include <Operators/OperatorId.hpp>
#include <Operators/OriginId.hpp>
#include <any>
#include <unordered_map>

namespace NES {

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class OperatorNode : public Node {
  public:
    explicit OperatorNode(OperatorId id);
    explicit OperatorNode(OperatorId id, std::vector<OriginId> inputOriginIds);

    ~OperatorNode() noexcept override = default;

    /**
     * @brief gets the operator id.
     * Unique Identifier of the operator within a query.
     * @return uint64_t
     */
    OperatorId getId() const;

    /**
     * NOTE: this method is only called from Logical Plan Expansion Rule
     * @brief gets the operator id.
     * Unique Identifier of the operator within a query.
     * @param operator id
     */
    void setId(OperatorId id);

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
     * @brief Remove this operator and joins the parent of this operator with the children of this operator.
     * @return bool true if successful
     */
    virtual bool removeAndJoinParentAndChildren();

    /**
     * @brief Add input operator as parent to this operator and move the parents of this node as parent to the input operator.
     * If the operator is already exists as parent then skip the operation
     * @return true if operation succeeded else false
     */
    bool insertBetweenThisAndParentNodes(const OperatorNodePtr& newOperator);

    /**
    * @brief Add input operator as child to this operator and add the input operator as new parent to the old operator's children
    * @return true if operation succeeded else false
    */
    bool insertBetweenThisAndChildNodes(const OperatorNodePtr& newOperator);

    /**
     * @brief replace an old operator with new operator
     * Old operator is the upstream operator of this operator, remove old operator as this operator's child and add new operator.
     * Assign all child operators of the old operator to the children of new operator.
     * If there's duplicated children among old and new operators, the children from new operator will overwrite them.
     * @param newOperator: Operator to replace with
     * @param oldOperator: operator to replace
     */
    bool replace(const OperatorNodePtr& newOperator, const OperatorNodePtr& oldOperator);

    /**
     * @brief replace current operator with new operator
     * @param newOperator: new operator to replace this operator with
     * @return true if successful
     */
    bool replace(const OperatorNodePtr& newOperator);

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
    void addProperty(const std::string& key, const std::any value);

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
    bool hasProperty(const std::string& key);

    /**
     * @brief Gets the output origin ids from this operator
     * @return std::vector<OriginId>
     */
    virtual std::vector<OriginId> getOutputOriginIds() = 0;

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
    OperatorId id;

    /*
     * @brief Map of properties of the current node
     */
    std::unordered_map<std::string, std::any> properties;
};

}// namespace NES

#endif// NES_INCLUDE_OPERATORS_OPERATORNODE_HPP_
