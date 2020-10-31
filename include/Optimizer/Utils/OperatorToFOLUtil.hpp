#ifndef NES_OPTIMIZE_UTILS_OPERATORTOFOLUTIL_HPP
#define NES_OPTIMIZE_UTILS_OPERATORTOFOLUTIL_HPP

#include <memory>

namespace z3{
class expr;
class context;
}

namespace NES{

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class SourceLogicalOperatorNode;
typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;

class SinkLogicalOperatorNode;
typedef std::shared_ptr<SinkLogicalOperatorNode> SinkLogicalOperatorNodePtr;

class WindowOperatorNode;
typedef std::shared_ptr<WindowOperatorNode> WindowOperatorNodePtr;

class SourceDescriptor;
typedef std::shared_ptr<SourceDescriptor> SourceDescriptorPtr;

class SinkDescriptor;
typedef std::shared_ptr<SinkDescriptor> SinkDescriptorPtr;


class OperatorToFOLUtil {
  public:
    /**
     * @brief Serializes an operator node and all its children to a SerializableOperator object.
     * @param operatorNode The operator node. Usually the root of the operator graph.
     * @param serializedParent The corresponding protobuff object, which is used to capture the state of the object.
     * @return the modified serializableOperator
     */
    static z3::expr serializeOperator(OperatorNodePtr operatorNode, z3::context &context);

    /**
    * @brief Serializes an source operator and all its properties to a SerializableOperator_SourceDetails object.
    * @param sourceOperator The source operator node.
    * @return the serialized SerializableOperator_SourceDetails
    */
    static z3::expr serializeSourceOperator(SourceLogicalOperatorNodePtr sourceOperator, z3::context &context);

    /**
     * @brief Serializes an sink operator and all its properties to a SerializableOperator_SinkDetails object.
     * @param sinkOperator The sink operator node.
     * @return the serialized SerializableOperator_SinkDetails.
     */
    static z3::expr serializeSinkOperator(SinkLogicalOperatorNodePtr sinkOperator, z3::context &context);

    /**
     * @brief Serializes an all window operator and all its properties to a SerializableOperator_WindowDetails object.
     * @param WindowLogicalOperatorNode The window operator node.
     * @return the serialized SerializableOperator_SinkDetails.
     */
    static z3::expr serializeWindowOperator(WindowOperatorNodePtr windowOperator, z3::context &context);

    /**
     * @brief Serializes an source descriptor and all its properties to a SerializableOperator_SourceDetails object.
     * @param sourceDescriptor The source descriptor.
     * @param sourceDetails The source details object.
     * @return the serialized SerializableOperator_SourceDetails.
     */
    static z3::expr serializeSourceSourceDescriptor(SourceDescriptorPtr sourceDescriptor, z3::context &context);

    /**
     * @brief Serializes an sink descriptor and all its properties to a SerializableOperator_SinkDetails object.
     * @param sinkDescriptor The sink descriptor.
     * @param sinkDetails The sink details object.
     * @return the serialized SerializableOperator_SinkDetails.
     */
    static z3::expr serializeSinkDescriptor(SinkDescriptorPtr sinkDescriptor, z3::context &context);

};
}

#endif//NES_OPTIMIZE_UTILS_OPERATORTOFOLUTIL_HPP
