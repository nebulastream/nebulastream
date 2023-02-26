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

#ifndef NES_CORE_INCLUDE_GRPC_SERIALIZATION_OPERATORSERIALIZATIONUTIL_HPP_
#define NES_CORE_INCLUDE_GRPC_SERIALIZATION_OPERATORSERIALIZATIONUTIL_HPP_

#include <Common/Identifiers.hpp>
#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>

#include <memory>
#include <SerializableOperator.pb.h>
#include <Operators/LogicalOperators/CEP/IterationLogicalOperatorNode.hpp>

namespace NES {

class SerializableOperator;
class SerializableOperator_SourceDetails;
class SerializableOperator_SinkDetails;
class SerializableOperator_WindowDetails;
class SerializableOperator_JoinDetails;
class SerializableOperator_BatchJoinDetails;
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
     * @param isClientOriginated Indicate if the source operator is originated from a client.
     * @return the modified serializableOperator
     */
    static SerializableOperator serializeOperator(const OperatorNodePtr& operatorNode, bool isClientOriginated = false);

    /**
     * @brief De-serializes the input SerializableOperator only
     * Note: This method will not deserialize its children
     * @param serializedOperator the serialized operator.
     * @return OperatorNodePtr
     */
    static OperatorNodePtr deserializeOperator(SerializableOperator serializedOperator);

    /**
    * @brief Serializes an source operator and all its properties to a SerializableOperator_SourceDetails object.
    * @param sourceOperator The source operator node.
    * @param isClientOriginated Indicate if the source operator is originated from a client.
    */
    static SerializableOperator_SourceDetails
    serializeSourceOperator(const SourceLogicalOperatorNode& sourceOperator, bool isClientOriginated = false);

    /**
     * @brief Deserializes a source logical operator
     * @param sourceDetails
     * @return SourceLogicalOperatorNode
     */
    static LogicalUnaryOperatorNodePtr
    deserializeSourceOperator(const SerializableOperator_SourceDetails& sourceDetails);

    /**
     * @brief serializes a filter operator node
     * @param filterOperator
     * @return SerializableOperator_FilterDetails
     */
    static SerializableOperator_FilterDetails serializeFilterOperator(const FilterLogicalOperatorNode& filterOperator);

    /**
     * @brief deserializes a filter operator node
     * @param filterDetails
     * @return filter operator node
     */
    static LogicalUnaryOperatorNodePtr deserializeFilterOperator(const SerializableOperator_FilterDetails& filterDetails);

    /**
     * @brief serializes a projection operator node
     * @param projectionOperator
     * @return SerializableOperator_ProjectionDetails
     */
    static SerializableOperator_ProjectionDetails
    serializeProjectionOperator(const ProjectionLogicalOperatorNode& projectionOperator);

    /**
     * @brief deserializes a projection operator node
     * @param projectionDetails
     * @return ProjectionLogicalOperatorNodePtr
     */
    static LogicalUnaryOperatorNodePtr
    deserializeProjectionOperator(const SerializableOperator_ProjectionDetails& projectionDetails);


    /**
     * @brief Serializes an sink operator and all its properties to a SerializableOperator_SinkDetails object.
     * @param sinkOperator The sink operator node.
     * @param sinkDetails the serialized SerializableOperator_SinkDetails.
     */
    static SerializableOperator_SinkDetails serializeSinkOperator(const SinkLogicalOperatorNode& sinkOperator);

    /**
     * @brief De-serializes the SerializableOperator_SinkDetails and all its properties back to a sink operatorNodePtr
     * @param sinkDetails The serialized sink operator details.
     * @return SinkLogicalOperatorNodePtr
     */
    static LogicalUnaryOperatorNodePtr deserializeSinkOperator(const SerializableOperator_SinkDetails& sinkDetails);

    /**
     * @brief serializes a map operator
     * @param mapOperator
     * @return SerializableOperator_MapDetails
     */
    static SerializableOperator_MapDetails serializeMapOperator(const MapLogicalOperatorNode& mapOperator);

    /**
     * @brief deserializes a map operator
     * @param mapDetails
     * @return MapLogicalOperatorNodePtr
     */
    static LogicalUnaryOperatorNodePtr deserializeMapOperator(const SerializableOperator_MapDetails& mapDetails);


    /**
     * @brief Serializes an all window operator and all its properties to a SerializableOperator_WindowDetails object.
     * @param WindowLogicalOperatorNode The window operator node.
     * @param windowDetails the serialized SerializableOperator_WindowDetails.
     */
    static SerializableOperator_WindowDetails serializeWindowOperator(const WindowOperatorNode& windowOperator);

    /**
     * @brief De-serializes the SerializableOperator_WindowDetails and all its properties back to a central window operatorNodePtr
     * @param windowDetails The serialized sink operator details.
     * @param operatorId: id of the operator to be deserialized
     * @return WindowOperatorNodePtr
     */
    static LogicalUnaryOperatorNodePtr
    deserializeWindowOperator(const SerializableOperator_WindowDetails& windowDetails, OperatorId operatorId);

    /**
     * @brief Serializes an all join operator and all its properties to a SerializableOperator_JoinDetails object.
     * @param JoinLogicalOperatorNodePtr The window operator node.
     * @return SerializableOperator_JoinDetails
     */
    static SerializableOperator_JoinDetails serializeJoinOperator(const JoinLogicalOperatorNode& joinOperator);

    /**
     * @brief De-serializes the SerializableOperator_JoinDetails and all its properties back to a join operatorNodePtr
     * @param sinkDetails The serialized sink operator details.
     * @param operatorId: id of the operator to be deserialized
     * @return JoinLogicalOperatorNode
     */
    static JoinLogicalOperatorNodePtr
    deserializeJoinOperator(const SerializableOperator_JoinDetails& joinDetails, OperatorId operatorId);

    /**
     * @brief Serializes an batch join operator and all its properties to a SerializableOperator_JoinDetails object.
     * @param BatchJoinLogicalOperatorNodePtr The window operator node.
     * @param batchJoinDetails the serialized SerializableOperator_BatchJoinDetails.
     */
    static SerializableOperator_BatchJoinDetails
    serializeBatchJoinOperator(const Experimental::BatchJoinLogicalOperatorNode& joinOperator);

    /**
     * @brief De-serializes the SerializableOperator_BatchJoinDetails and all its properties back to a join operatorNodePtr
     * @param sinkDetails The serialized sink operator details.
     * @param operatorId: id of the operator to be deserialized
     * @return BatchJoinLogicalOperatorNode
     */
    static Experimental::BatchJoinLogicalOperatorNodePtr
    deserializeBatchJoinOperator(const SerializableOperator_BatchJoinDetails& joinDetails, OperatorId operatorId);


    /**
     * @brief Serializes an source descriptor and all its properties to a SerializableOperator_SourceDetails object.
     * @param sourceDescriptor The source descriptor.
     * @param sourceDetails the
     * @param isClientOriginated Indicate if the source operator is originated from a client
     * @return sourceDetails The source details object.
     */
    static void serializeSourceDescriptor(const SourceDescriptor& sourceDescriptor,
                                          SerializableOperator_SourceDetails& sourceDetails,
                                          bool isClientOriginated = false);

    /**
     * @brief De-serializes the SerializableOperator_SourceDetails and all its properties back to a sink SourceDescriptorPtr.
     * @param sourceDetails The serialized source operator details.
     * @return SourceDescriptorPtr
     */
    static SourceDescriptorPtr deserializeSourceDescriptor(const SerializableOperator_SourceDetails& sourceDetails);

    /**
     * @brief Serializes an sink descriptor and all its properties to a SerializableOperator_SinkDetails object.
     * @param sinkDescriptor The sink descriptor.
     * @param sinkDetails The sink details object.
     * @param numberOfOrigins the number of origins
     */
    static void serializeSinkDescriptor(const SinkDescriptor& sinkDescriptor,
                                        SerializableOperator_SinkDetails& sinkDetails,
                                        uint64_t numberOfOrigins);

    /**
     * @brief De-serializes the SerializableOperator_SinkDetails and all its properties back to a sink SinkDescriptorPtr.
     * @param sinkDetails The serialized sink operator details.
     * @return OperatorNodePtr
     */
    static SinkDescriptorPtr deserializeSinkDescriptor(const SerializableOperator_SinkDetails& sinkDetails);

    /*
     * @brief Serializes the watermarkAssigner operator
     * @param watermark assigner logical operator node
     */
    static SerializableOperator_WatermarkStrategyDetails
    serializeWatermarkAssignerOperator(const WatermarkAssignerLogicalOperatorNode& watermarkAssignerOperator);

    /**
     * @brief Deserializes a watermarkAssigner operator
     * @param watermarkStrategyDetails
     * @return watermarkAssigner operator
     */
    static LogicalUnaryOperatorNodePtr
    deserializeWatermarkAssignerOperator(const SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails);

    /*
     * @brief Serializes a watermark strategy descriptor
     * @param watermarkStrategyDescriptor The watermark strategy descriptor
     * @param watermarkStrategyDetails The watermark strategy details object
     */
    static void
    serializeWatermarkStrategyDescriptor(const Windowing::WatermarkStrategyDescriptor& watermarkStrategyDescriptor,
                                         SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails);

    /*
     * @brief de-serialize to WatermarkStrategyDescriptor
     * @param watermarkStrategyDetails details of serializable watermarkstrategy
     * @return WatermarkStrategyDescriptor
     */
    static Windowing::WatermarkStrategyDescriptorPtr
    deserializeWatermarkStrategyDescriptor(const SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails);

    /**
     * @brief serializes an input schema
     * @param operatorNode
     * @param serializedOperator
     */
    static void serializeInputSchema(const OperatorNodePtr &operatorNode, SerializableOperator &serializedOperator);

    /**
     * @brief deserializes an input schema
     * @param serializedOperator
     * @param operatorNode
     */
    static void deserializeInputSchema(LogicalOperatorNodePtr operatorNode,
                                       const SerializableOperator& serializedOperator);

    /**
     * @brief Serializes an inferModel logical operator
     * @param inferModel operator
     * @param serializedOperator serialized operator
     */
    static SerializableOperator_InferModelDetails
    serializeInferModelOperator(const InferModel::InferModelLogicalOperatorNode& inferModel);

    /**
     * @brief deserializes an inferModel logical operator
     * @param inferModelDetails
     * @return
     */
    static LogicalUnaryOperatorNodePtr
    deserializeInferModelOperator(const SerializableOperator_InferModelDetails& inferModelDetails);

    /**
     * @brief serializes a CEPIteration operator
     * @param iterationOperator
     * @return SerializableOperator_CEPIterationDetails
     */
    static SerializableOperator_CEPIterationDetails
    serializeCEPIterationOperator(const IterationLogicalOperatorNode& iterationOperator);

    /**
     * @brief deserializes a CEPIteration operator
     * @param cepIterationDetails
     * @return IterationLogicalOperatorNodePtr
     */
    static LogicalUnaryOperatorNodePtr
    deserializeCEPIterationOperator(const SerializableOperator_CEPIterationDetails& cepIterationDetails);

    /**
     * @brief serializes a mapJavaUdf operator
     * @param mapJavaUdfOperatorNode
     * @return SerializableOperator_MapJavaUdfDetails
     */
    static SerializableOperator_MapJavaUdfDetails
    serializeMapJavaUdfOperator(const MapJavaUdfLogicalOperatorNode& mapJavaUdfOperatorNode);

    /**
     * @brief deserializes a mapJavaUdf operator
     * @param mapJavaUdfDetails
     * @return MapJavaUdfLogicalOperatorNodePtr
     */
    static LogicalUnaryOperatorNodePtr
    deserializeMapJavaUdfOperator(const SerializableOperator_MapJavaUdfDetails& mapJavaUdfDetails);

};

}// namespace NES

#endif// NES_CORE_INCLUDE_GRPC_SERIALIZATION_OPERATORSERIALIZATIONUTIL_HPP_
