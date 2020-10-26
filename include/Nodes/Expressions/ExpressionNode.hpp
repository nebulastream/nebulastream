#ifndef NES_INCLUDE_NODES_EXPRESSIONS_EXPRESSION_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_EXPRESSION_HPP_
#include <Nodes/Node.hpp>
#include <memory>
namespace NES {

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

/**
 * @brief this indicates an expression, which is a parameter for a FilterOperator or a MapOperator.
 * Each expression declares a stamp, which expresses the data type of this expression.
 * A stamp can be of a concrete type or invalid if the data type was not yet inferred.
 */
class ExpressionNode : public Node {

  public:
    ExpressionNode(DataTypePtr stamp);

    ~ExpressionNode() = default;

    /**
     * @brief Indicates if this expression is a predicate -> if its result stamp is a boolean
     * @return
     */
    bool isPredicate();

    /**
     * @brief Infers the stamp of the expression given the current schema.
     * @param SchemaPtr
     */
    virtual void inferStamp(SchemaPtr schema);

    /**
     * @brief returns the stamp as the data type which is produced by this expression.
     * @return Stamp
     */
    const DataTypePtr getStamp() const;

    /**
     * @brief sets the stamp of this expression.
     * @param stamp
     */
    void setStamp(DataTypePtr stamp);

    /**
     * @brief Create a deep copy of this expression node.
     * @return ExpressionNodePtr
     */
    virtual ExpressionNodePtr copy() = 0;

  protected:
    ExpressionNode(ExpressionNode* other);

    /**
     * @brief declares the type of this expression.
     * todo replace the direct usage of data types with a stamp abstraction.
     */
    DataTypePtr stamp;
};
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;
}// namespace NES
#endif//NES_INCLUDE_NODES_EXPRESSIONS_EXPRESSION_HPP_
