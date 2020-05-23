#ifndef NES_INCLUDE_UTIL_OPERATORSERIALIZATIONUTIL_HPP_
#define NES_INCLUDE_UTIL_OPERATORSERIALIZATIONUTIL_HPP_

#include <memory>

namespace NES {

class SerializableOperator;
typedef std::shared_ptr<SerializableOperator> SerializableOperatorPtr;

class SerializableOperator_SourceDetails;
class SerializableOperator_SinkDetails;

class LogicalOperatorNode;
typedef std::shared_ptr<LogicalOperatorNode> LogicalOperatorNodePtr;

class SourceLogicalOperatorNode;
typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;

class SinkLogicalOperatorNode;
typedef std::shared_ptr<SinkLogicalOperatorNode> SinkLogicalOperatorNodePtr;

class SourceDescriptor;
typedef std::shared_ptr<SourceDescriptor> SourceDescriptorPtr;

class SinkDescriptor;
typedef std::shared_ptr<SinkDescriptor> SinkDescriptorPtr;

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

    static SerializableOperator* serializeOperator(NodePtr parent, SerializableOperator* serializedParent);
    static SerializableOperator_SourceDetails serializeSourceOperator(SourceLogicalOperatorNodePtr sourceOperator);
    static LogicalOperatorNodePtr deserializeSourceOperator(SerializableOperator_SourceDetails* serializedSourceDetails);
    static SerializableOperator_SinkDetails serializeSinkOperator(SinkLogicalOperatorNodePtr sinkOperator);
    static LogicalOperatorNodePtr deserializeSinkOperator(SerializableOperator_SinkDetails* sinkDetails);

    static SerializableOperator_SourceDetails* serializeSourceSourceDescriptor(SourceDescriptorPtr sourceDescriptor, SerializableOperator_SourceDetails* serializedSourceDetails);
    static SourceDescriptorPtr deserializeSourceDescriptor(SerializableOperator_SourceDetails* serializedSourceDetails);

    static SerializableOperator_SinkDetails* serializeSinkDescriptor(SinkDescriptorPtr sinkDescriptor, SerializableOperator_SinkDetails* sinkDetails);
    static SinkDescriptorPtr deserializeSinkDescriptor(SerializableOperator_SinkDetails* sinkDetails);
};

}// namespace NES

#endif//NES_INCLUDE_UTIL_OPERATORSERIALIZATIONUTIL_HPP_
