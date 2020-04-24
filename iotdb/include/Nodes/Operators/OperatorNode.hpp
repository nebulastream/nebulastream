#ifndef NES_INCLUDE_NODES_OPERATORS_OPERATORNODE_HPP_
#define NES_INCLUDE_NODES_OPERATORS_OPERATORNODE_HPP_

#include <Nodes/Node.hpp>

namespace NES {

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class OperatorNode : public Node {
  public:
    OperatorNode();

    /**
    * @brief get the input schema of this operator
    * @return SchemaPtr
    */
    SchemaPtr getInputSchema() const;

    /**
    * @brief get the result schema of this operator
    * @return SchemaPtr
    */
    SchemaPtr getOutputSchema() const;

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
     * @brief gets the operator id.
     * Unique Identifier of the operator within a query.
     * @param operator id
     */
    void setId(u_int64_t id);

  protected:

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

typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

}

#endif //NES_INCLUDE_NODES_OPERATORS_OPERATORNODE_HPP_
