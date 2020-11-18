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

#ifndef NES_INCLUDE_UTIL_OPERATORSERIALIZATIONUTIL_HPP_
#define NES_INCLUDE_UTIL_OPERATORSERIALIZATIONUTIL_HPP_

#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/OperatorId.hpp>

#include <memory>
namespace NES {

class SerializableOperator;
class SerializableOperator_SourceDetails;
class SerializableOperator_SinkDetails;
class SerializableOperator_WindowDetails;
class SerializableOperator_JoinDetails;
class SerializableOperator_WatermarkStrategyDetails;

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
     * @brief De-serializes the input SerializableOperator only
     * Note: This method will not deserialize its children
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
    static OperatorNodePtr deserializeSourceOperator(SerializableOperator_SourceDetails* sourceDetails, OperatorId operatorId);

    /**
     * @brief Serializes an sink operator and all its properties to a SerializableOperator_SinkDetails object.
     * @param sinkOperator The sink operator node.
     * @return the serialized SerializableOperator_SinkDetails.
     */
    static SerializableOperator_SinkDetails serializeSinkOperator(SinkLogicalOperatorNodePtr sinkOperator);

    /**
     * @brief Serializes an all window operator and all its properties to a SerializableOperator_WindowDetails object.
     * @param WindowLogicalOperatorNode The window operator node.
     * @return the serialized SerializableOperator_WindowDetails.
     */
    static SerializableOperator_WindowDetails serializeWindowOperator(WindowOperatorNodePtr windowOperator);

    /**
     * @brief Serializes an all join operator and all its properties to a SerializableOperator_JoinDetails object.
     * @param JoinLogicalOperatorNodePtr The window operator node.
     * @return the serialized SerializableOperator_SinkDetails.
     */
    static SerializableOperator_JoinDetails serializeJoinOperator(JoinLogicalOperatorNodePtr joinOperator);

    /**
     * @brief De-serializes the SerializableOperator_SinkDetails and all its properties back to a sink operatorNodePtr
     * @param sinkDetails The serialized sink operator details.
     * @return SinkLogicalOperatorNodePtr
     */
    static OperatorNodePtr deserializeSinkOperator(SerializableOperator_SinkDetails* sinkDetails);

    /**
     * @brief De-serializes the SerializableOperator_WindowDetails and all its properties back to a central window operatorNodePtr
     * @param windowDetails The serialized sink operator details.
     * @param operatorId: id of the operator to be deserialized
     * @return WindowOperatorNodePtr
     */
    static WindowOperatorNodePtr deserializeWindowOperator(SerializableOperator_WindowDetails* windowDetails,
                                                           OperatorId operatorId);

    /**
     * @brief De-serializes the SerializableOperator_JoinDetails and all its properties back to a join operatorNodePtr
     * @param sinkDetails The serialized sink operator details.
     * @param operatorId: id of the operator to be deserialized
     * @return JoinLogicalOperatorNode
     */
    static JoinLogicalOperatorNodePtr deserializeJoinOperator(SerializableOperator_JoinDetails* joinDetails,
                                                              OperatorId operatorId);

    /**
     * @brief Serializes an source descriptor and all its properties to a SerializableOperator_SourceDetails object.
     * @param sourceDescriptor The source descriptor.
     * @param sourceDetails The source details object.
     * @return the serialized SerializableOperator_SourceDetails.
     */
    static SerializableOperator_SourceDetails* serializeSourceSourceDescriptor(SourceDescriptorPtr sourceDescriptor,
                                                                               SerializableOperator_SourceDetails* sourceDetails);

    /**
     * @brief De-serializes the SerializableOperator_SourceDetails and all its properties back to a sink SourceDescriptorPtr.
     * @param sourceDetails The serialized source operator details.
     * @return SourceDescriptorPtr
     */
    static SourceDescriptorPtr deserializeSourceDescriptor(SerializableOperator_SourceDetails* sourceDetails,
                                                           OperatorId operatorId);

    /**
     * @brief Serializes an sink descriptor and all its properties to a SerializableOperator_SinkDetails object.
     * @param sinkDescriptor The sink descriptor.
     * @param sinkDetails The sink details object.
     * @return the serialized SerializableOperator_SinkDetails.
     */
    static SerializableOperator_SinkDetails* serializeSinkDescriptor(SinkDescriptorPtr sinkDescriptor,
                                                                     SerializableOperator_SinkDetails* sinkDetails);

    /**
     * @brief De-serializes the SerializableOperator_SinkDetails and all its properties back to a sink SinkDescriptorPtr.
     * @param sinkDetails The serialized sink operator details.
     * @return SinkDescriptorPtr
     */
    static SinkDescriptorPtr deserializeSinkDescriptor(SerializableOperator_SinkDetails* sinkDetails);

    /*
     * @brief Serializes the watermarkAssigner operator
     * @param watermark assigner logical operator node
     * @return serialized watermark operator
     */
    static SerializableOperator_WatermarkStrategyDetails
    serializeWatermarkAssignerOperator(WatermarkAssignerLogicalOperatorNodePtr watermarkAssignerOperator);

    /*
     * @brief Serializes a watermark strategy descriptor
     * @brief watermarkStrategyDescriptor The watermark strategy descriptor
     * @param watermarkDetails The watermark strategy details object
     * @return  the serialized watermark strategy
     */
    static SerializableOperator_WatermarkStrategyDetails*
    serializeWatermarkStrategyDescriptor(Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor,
                                         SerializableOperator_WatermarkStrategyDetails* watermarkStrategyDetails);

    /*
     * @brief de-serialize to WatermarkStrategyDescriptor
     * @param watermarkStrategyDetails details of serializable watermarkstrategy
     * @return WatermarkStrategyDescriptor
     */
    static Windowing::WatermarkStrategyDescriptorPtr
    deserializeWatermarkStrategyDescriptor(SerializableOperator_WatermarkStrategyDetails* watermarkStrategyDetails);
};

}// namespace NES

#endif//NES_INCLUDE_UTIL_OPERATORSERIALIZATIONUTIL_HPP_
