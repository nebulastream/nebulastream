#ifndef NES_INCLUDE_UTIL_OPERATORSERIALIZATIONUTIL_HPP_
#define NES_INCLUDE_UTIL_OPERATORSERIALIZATIONUTIL_HPP_

#include <memory>

namespace NES {

class SourceLogicalOperatorNode;
typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;

class SinkLogicalOperatorNode;
typedef std::shared_ptr<SinkLogicalOperatorNode> SinkLogicalOperatorNodePtr;

class WindowLogicalOperatorNode;
typedef std::shared_ptr<WindowLogicalOperatorNode> WindowLogicalOperatorNodePtr;

class SourceDescriptor;
typedef std::shared_ptr<SourceDescriptor> SourceDescriptorPtr;

class SinkDescriptor;
typedef std::shared_ptr<SinkDescriptor> SinkDescriptorPtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class SerializableOperator;
class SerializableOperator_SourceDetails;
class SerializableOperator_SinkDetails;
class SerializableOperator_WindowDetails;

/**
 * @brief The OperatorSerializationUtil offers functionality to serialize and de-serialize logical operator trees to a
 * corresponding protobuffer object.
 */
class OperatorSerializationUtil {
  public:
    /**
     * @brief Serializes an operator node and all its children to a SerializableOperator object.
     * @param operatorNode The operator node. Usually the root of the operator graph.
     * @param serializedParent The corresponding protobuff object, which is used to capture the state of the object.
     * @return the modified serializableOperator
     */
    static SerializableOperator* serializeOperator(OperatorNodePtr operatorNode, SerializableOperator* serializableOperator);

    /**
     * @brief De-serializes the SerializableOperator and all its children back to a OperatorNodePtr
     * @param serializedOperator the serialized operator.
     * @return OperatorNodePtr
     */
    static OperatorNodePtr deserializeOperator(SerializableOperator* serializedOperator);

    /**
    * @brief Serializes an source operator and all its properties to a SerializableOperator_SourceDetails object.
    * @param sourceOperator The source operator node.
    * @return the serialized SerializableOperator_SourceDetails
    */
    static SerializableOperator_SourceDetails serializeSourceOperator(SourceLogicalOperatorNodePtr sourceOperator);

    /**
     * @brief De-serializes the SerializableOperator_SourceDetails and all its properties back to a source operatorNodePtr
     * @param sourceDetails The serialized source operator details.
     * @return SourceLogicalOperatorNodePtr
     */
    static OperatorNodePtr deserializeSourceOperator(SerializableOperator_SourceDetails* sourceDetails);

    /**
     * @brief Serializes an sink operator and all its properties to a SerializableOperator_SinkDetails object.
     * @param sinkOperator The sink operator node.
     * @return the serialized SerializableOperator_SinkDetails.
     */
    static SerializableOperator_SinkDetails serializeSinkOperator(SinkLogicalOperatorNodePtr sinkOperator);

    /**
     * @brief Serializes an window operator and all its properties to a SerializableOperator_WindowDetails object.
     * @param windowOperator The window operator node.
     * @return the serialized SerializableOperator_SinkDetails.
     */
    static SerializableOperator_WindowDetails serializeWindowOperator(WindowLogicalOperatorNodePtr windowOperator);

    /**
     * @brief De-serializes the SerializableOperator_SinkDetails and all its properties back to a sink operatorNodePtr
     * @param sinkDetails The serialized sink operator details.
     * @return SinkLogicalOperatorNodePtr
     */
    static OperatorNodePtr deserializeSinkOperator(SerializableOperator_SinkDetails* sinkDetails);

    /**
     * @brief De-serializes the SerializableOperator_WindowDetails and all its properties back to a window operatorNodePtr
     * @param sinkDetails The serialized sink operator details.
     * @return SinkLogicalOperatorNodePtr
     */
    static WindowLogicalOperatorNodePtr deserializeWindowOperator(SerializableOperator_WindowDetails* sinkDetails);

    /**
     * @brief Serializes an source descriptor and all its properties to a SerializableOperator_SourceDetails object.
     * @param sourceDescriptor The source descriptor.
     * @param sourceDetails The source details object.
     * @return the serialized SerializableOperator_SourceDetails.
     */
    static SerializableOperator_SourceDetails* serializeSourceSourceDescriptor(SourceDescriptorPtr sourceDescriptor, SerializableOperator_SourceDetails* sourceDetails);

    /**
     * @brief De-serializes the SerializableOperator_SourceDetails and all its properties back to a sink SourceDescriptorPtr.
     * @param sourceDetails The serialized source operator details.
     * @return SourceDescriptorPtr
     */
    static SourceDescriptorPtr deserializeSourceDescriptor(SerializableOperator_SourceDetails* sourceDetails);

    /**
     * @brief Serializes an sink descriptor and all its properties to a SerializableOperator_SinkDetails object.
     * @param sinkDescriptor The sink descriptor.
     * @param sinkDetails The sink details object.
     * @return the serialized SerializableOperator_SinkDetails.
     */
    static SerializableOperator_SinkDetails* serializeSinkDescriptor(SinkDescriptorPtr sinkDescriptor, SerializableOperator_SinkDetails* sinkDetails);

    /**
     * @brief De-serializes the SerializableOperator_SinkDetails and all its properties back to a sink SinkDescriptorPtr.
     * @param sinkDetails The serialized sink operator details.
     * @return SinkDescriptorPtr
     */
    static SinkDescriptorPtr deserializeSinkDescriptor(SerializableOperator_SinkDetails* sinkDetails);


};

}// namespace NES

#endif//NES_INCLUDE_UTIL_OPERATORSERIALIZATIONUTIL_HPP_
