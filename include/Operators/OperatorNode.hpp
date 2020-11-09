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

#ifndef NES_INCLUDE_NODES_OPERATORS_OPERATORNODE_HPP_
#define NES_INCLUDE_NODES_OPERATORS_OPERATORNODE_HPP_

#include <Nodes/Node.hpp>
#include <Operators/OperatorId.hpp>

namespace NES {

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class OperatorNode : public Node {
  public:
    OperatorNode(OperatorId id);

    /**
    * @brief get the input schema of this operator
    * @return SchemaPtr
    */
    SchemaPtr getInputSchema() const;
    void setInputSchema(SchemaPtr inputSchema);

    /**
    * @brief get the result schema of this operator
    * @return SchemaPtr
    */
    SchemaPtr getOutputSchema() const;
    void setOutputSchema(SchemaPtr outputSchema);

    /**
    * @brief infers the input and out schema of this operator depending on its child.
    * @throws Exception if the schema could not be infers correctly or if the inferred types are not valid.
    * @return true if schema was correctly inferred
    */
    virtual bool inferSchema();

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
    bool isNAryOperator();

    bool addChild(const NodePtr newNode) override;

    bool addParent(const NodePtr newNode) override;

    /**
     * @brief Get the operator with input operator id
     * @param operatorId : the if of the operator to find
     * @return nullptr if not found else the operator node
     */
    NodePtr getChildWithOperatorId(uint64_t operatorId);

  protected:
    /**
     * @brief get duplicate of the input operator and all its ancestors
     * @param operatorNode: the input operator
     * @return duplicate of the input operator
     */
    OperatorNodePtr getDuplicateOfParent(OperatorNodePtr operatorNode);

    /**
     * @brief get duplicate of the input operator and all its children
     * @param operatorNode: the input operator
     * @return duplicate of the input operator
     */
    OperatorNodePtr getDuplicateOfChild(OperatorNodePtr operatorNode);

    /**
     * @brief Unique Identifier of the operator within a query.
     */
    u_int64_t id;

    /**
     * @brief All operators maintain an input and output schema.
     * To make sure that the schema is propagated correctly through the operator chain all inferSchema();
     */
    SchemaPtr inputSchema;
    SchemaPtr outputSchema;
};

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_OPERATORNODE_HPP_
