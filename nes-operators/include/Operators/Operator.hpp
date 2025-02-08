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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_OPERATOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_OPERATOR_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <any>
#include <unordered_map>

namespace NES {

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class Operator;
using OperatorPtr = std::shared_ptr<Operator>;
using OperatorProperties = std::unordered_map<std::string, std::any>;

/**
 * @brief Returns the next free operator id
 * @return operator id
 */
OperatorId getNextOperatorId();

/**
 * @brief Returns the next free statistic id
 * @return StatisticId
 */
StatisticId getNextStatisticId();

class Operator : public Node {
  public:
    explicit Operator(OperatorId id);
    explicit Operator(OperatorId id, StatisticId statisticId);

    ~Operator() noexcept override = default;

    /**
     * @brief gets the operator id.
     * Unique Identifier of the operator within a query.
     * @return uint64_t
     */
    OperatorId getId() const;

    /**
     * @brief Gets the statisticId of this operator for example to pass it down to the physical operator
     * @return StatisticId
     */
    StatisticId getStatisticId() const;

    /**
     * NOTE: this method is only called from Logical Plan Expansion Rule
     * @brief gets the operator id.
     * Unique Identifier of the operator within a query.
     * @param operator id
     */
    void setId(OperatorId id);

    /**
     * @brief Sets the statistic id
     * @param statisticId: represents the unique identifier of components that we can track statistics for
     */
    void setStatisticId(StatisticId statisticId);

    /**
     * @brief Create duplicate of this operator by copying its context information and also its parent and child operator set.
     * @return duplicate of this logical operator
     */
    OperatorPtr duplicate();

    /**
     * @brief Create a shallow copy of the operator by copying its operator properties but not its children or parent operator tree.
     * @return shallow copy of the operator
     */
    virtual OperatorPtr copy() = 0;

    /**
     * @brief detect if this operator is a n-ary operator, i.e., it has multiple parent or children.
     * @return true if n-ary else false;
     */
    bool hasMultipleChildrenOrParents() const;

    /**
    * @brief return if the operator has multiple children
    * @return bool
    */
    bool hasMultipleChildren() const;

    /**
    * @brief return if the operator has multiple children
    * @return bool
    */
    bool hasMultipleParents() const;

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
     * @param operatorId : the operator id of the operator to find
     * @return nullptr if not found else the operator node
     */
    NodePtr getChildWithOperatorId(OperatorId operatorId) const;

    /**
     * @brief Get the operator with statisticId id
     * @param operatorId : the statisticId id of the operator to find
     * @return nullptr if not found else the operator node
     */
    NodePtr getChildWithStatisticId(StatisticId statisticId) const;

    /**
     * Check if a node with the id is either a child or a grandchild of this operator
     * @param operatorNode : the operator node whose id need to be looked up
     * @return true if operator with the input id is a child or grand child
     */
    bool containAsGrandChild(NodePtr operatorNode) override;

    /**
     * Check if a node with the id is either a parent or a grandparent of this operator
     * @param operatorNode : the operator node whose id need to be looked up
     * @return true if operator with the input id is a child or grand child
     */
    bool containAsGrandParent(NodePtr operatorNode) override;

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
     * @brief Add a new property string to the stored properties map
     * @param key key of the new property
     * @param value value of the new property
     */
    void addProperty(const std::string& key, const std::any value);

    /**
     * @brief Add a set of properties to this operator
     * @param properties
     */
    void addAllProperties(const OperatorProperties& properties);

    /**
     * @brief Get a the value of a property
     * @param key key of the value to retrieve
     * @return value of the property with the given key
     */
    std::any getProperty(const std::string& key) const;

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
    bool hasProperty(const std::string& key) const;

    /**
     * @brief Gets the output origin ids from this operator
     * @return std::vector<OriginId>
     */
    virtual std::vector<OriginId> getOutputOriginIds() const = 0;

    /**
     * @brief returns the string representation of the class
     * @return the string representation of the class
     */
    std::string toString() const override;

  protected:
    /**
     * @brief get duplicate of the input operator and all its ancestors
     * @param operatorNode: the input operator
     * @return duplicate of the input operator
     */
    OperatorPtr getDuplicateOfParent(const OperatorPtr& operatorNode);

    /**
     * @brief get duplicate of the input operator and all its children
     * @param operatorNode: the input operator
     * @return duplicate of the input operator
     */
    OperatorPtr getDuplicateOfChild(const OperatorPtr& operatorNode);

    /**
     * @brief Unique Identifier of the operator within a query.
     */
    OperatorId id;

    /**
     * @brief Unique Identifier of the operator across the whole system.
     */
    StatisticId statisticId;

    /*
     * @brief Map of properties of the current node
     */
    OperatorProperties properties;
};

}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_OPERATOR_HPP_
