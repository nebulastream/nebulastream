#ifndef NES_INCLUDE_UTIL_OPERATORSERIALIZATIONUTIL_HPP_
#define NES_INCLUDE_UTIL_OPERATORSERIALIZATIONUTIL_HPP_

#include <memory>

namespace NES {

class SerializableOperator;
typedef std::shared_ptr<SerializableOperator> SerializableOperatorPtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class SerializableSchema;
class SerializableDataType;
class Node;
typedef std::shared_ptr<Node> NodePtr;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class OperatorSerializationUtil {
  public:
    SerializableOperatorPtr serialize(QueryPlanPtr plan);

    void serializeNode(NodePtr node, SerializableOperator* serializedOperator){};

    void serializeSchema(SchemaPtr schema, SerializableSchema* serializedSchema){};

    void serializeOperator(NodePtr parent, SerializableOperator* serializedParent){};
};

}// namespace NES

#endif//NES_INCLUDE_UTIL_OPERATORSERIALIZATIONUTIL_HPP_
