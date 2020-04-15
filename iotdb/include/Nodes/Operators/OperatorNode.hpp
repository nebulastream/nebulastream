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
    * @brief get the result schema of this operator
    * @return SchemaPtr
    */
    virtual SchemaPtr getResultSchema() const;

    u_int64_t getId() const;
    void setId(u_int64_t id);

  private:

    /**
     * @brief Unique Identifier of the operator within a query.
     */
    u_int64_t id;
};

typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

}

#endif //NES_INCLUDE_NODES_OPERATORS_OPERATORNODE_HPP_
