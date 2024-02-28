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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_SERIALIZATION_OPERATORSERIALIZATIONUTIL_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_SERIALIZATION_OPERATORSERIALIZATIONUTIL_HPP_

#include <Identifiers.hpp>
#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>
#include <Operators/LogicalOperators/UDFs/FlatMapUDF/FlatMapUDFLogicalOperator.hpp>
#include <Operators/LogicalOperators/UDFs/MapUDF/MapUDFLogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalOpenCLOperator.hpp>
#include <SerializableOperator.pb.h>
#include <memory>

namespace NES {

class SerializableOperator;
class SerializableOperator_SourceDetails;
class SerializableOperator_SinkDetails;
class SerializableOperator_WindowDetails;
class SerializableOperator_JoinDetails;
class SerializableOperator_BatchJoinDetails;
class SerializableOperator_WatermarkStrategyDetails;
class SerializableOperator_LimitDetails;
class SerializableOperator_MapDetails;
class SerializableOperator_InferModelDetails;
class SerializableOperator_MapJavaUdfDetails;
class SerializableOperator_FlatMapJavaUdfDetails;
class SerializableOperator_JavaUdfWindowDetails;
class SerializableOperator_CEPIterationDetails;
class SerializableOperator_ProjectionDetails;
class SerializableOperator_FilterDetails;
class SerializableOperator_UnionDetails;
class SerializableOperator_BroadcastDetails;

/**
 * @brief The OperatorSerializationUtil offers functionality to serialize and deserialize logical operator trees to a
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
    static SerializableOperator serializeOperator(const OperatorPtr& operatorNode, bool isClientOriginated = false);

    /**
     * @brief Deserializes the input SerializableOperator only
     * Note: This method will not deserialize its children
     * @param serializedOperator the serialized operator.
     * @return OperatorPtr
     */
    static OperatorPtr deserializeOperator(SerializableOperator serializedOperator);

    /**
    * @brief Serializes an source operator and all its properties to a SerializableOperator_SourceDetails object.
    * @param sourceOperator The source operator node.
    * @param isClientOriginated Indicate if the source operator is originated from a client.
    * @param serializedOperator serialized instance of the operator
    */
    static void serializeSourceOperator(const SourceLogicalOperator& sourceOperator,
                                        SerializableOperator& serializedOperator,
                                        bool isClientOriginated = false);

    /**
     * @brief Deserializes a source logical operator and all its properties to a SourceLogicalOperator
     * @param sourceDetails The serialized source operator
     * @return SourceLogicalOperator of type LogicalUnaryOperator
     */
    static LogicalUnaryOperatorPtr deserializeSourceOperator(const SerializableOperator_SourceDetails& sourceDetails);

    /**
     * @brief Serializes a filter operator node and all its properties to a SerializableOperator_FilterDetails object.
     * @param filterOperator the LogicalFilterOperator
     * @param serializedOperator serialized instance of the operator
     */
    static void serializeFilterOperator(const LogicalFilterOperator& filterOperator,
                                        SerializableOperator& serializedOperator);

    /**
     * @brief Deserializes a SerializableOperator_FilterDetails and all its properties to a LogicalFilterOperator
     * @param filterDetails The serialized filterDetails
     * @return LogicalFilterOperator of type LogicalUnaryOperator
     */
    static LogicalUnaryOperatorPtr deserializeFilterOperator(const SerializableOperator_FilterDetails& filterDetails);

    /**
     * @brief Serializes a projection operator node and all its properties to a SerializableOperator_ProjectionDetails
     * @param projectionOperator the LogicalProjectionOperator
     * @param serializedOperator serialized instance of the operator
     */
    static void serializeProjectionOperator(const LogicalProjectionOperator& projectionOperator,
                                            SerializableOperator& serializedOperator);

    /**
     * @brief Deserializes a projection operator node and all its properties to a LogicalProjectionOperatorPtr
     * @param projectionDetails the serialized projectionDetails
     * @return LogicalProjectionOperator of type LogicalUnaryOperator
     */
    static LogicalUnaryOperatorPtr
    deserializeProjectionOperator(const SerializableOperator_ProjectionDetails& projectionDetails);

    /**
     * @brief Serializes an sink operator and all its properties to a SerializableOperator_SinkDetails object.
     * @param sinkOperator The sink operator node.
     * @param serializedOperator serialized instance of the operator
     */
    static void serializeSinkOperator(const SinkLogicalOperator& sinkOperator, SerializableOperator& serializedOperator);

    /**
     * @brief Deserializes the SerializableOperator_SinkDetails and all its properties back to a sink operatorNodePtr
     * @param sinkDetails The serialized sink operator details.
     * @return SinkLogicalOperatorPtr of type LogicalUnaryOperator
     */
    static LogicalUnaryOperatorPtr deserializeSinkOperator(const SerializableOperator_SinkDetails& sinkDetails);

    /**
     * @brief Serializes a map operator and all its properties to a SerializableOperator_MapDetails
     * @param mapOperator the map operator
     * @param serializedOperator serialized instance of the operator
     */
    static void serializeMapOperator(const LogicalMapOperator& mapOperator, SerializableOperator& serializedOperator);

    /**
     * @brief Deserializes a map operator and all its properties back to a LogicalMapOperator
     * @param mapDetails the serialized instance of a LogicalMapOperator
     * @return LogicalMapOperatorPtr of type LogicalUnaryOperator
     */
    static LogicalUnaryOperatorPtr deserializeMapOperator(const SerializableOperator_MapDetails& mapDetails);

    /**
     * @brief Serializes an all window operator and all its properties to a SerializableOperator_WindowDetails object.
     * @param LogicalWindowOperator The window operator node.
     * @param windowDetails the serialized SerializableOperator_WindowDetails.
     * @param serializedOperator serialized instance of the operator
     */
    static void serializeWindowOperator(const WindowOperator& windowOperator, SerializableOperator& serializedOperator);

    /**
     * @brief Deserializes the SerializableOperator_WindowDetails and all its properties back to a central window operatorNodePtr
     * @param windowDetails The serialized sink operator details.
     * @param operatorId: id of the operator to be deserialized
     * @return WindowOperatorPtr of type LogicalUnaryOperator
     */
    static LogicalUnaryOperatorPtr deserializeWindowOperator(const SerializableOperator_WindowDetails& windowDetails,
                                                                 OperatorId operatorId);

    /**
     * @brief Serializes an all join operator and all its properties to a SerializableOperator_JoinDetails object.
     * @param LogicalJoinOperatorPtr The window operator node.
     * @param serializedOperator serialized instance of the operator
     */
    static void serializeJoinOperator(const LogicalJoinOperator& joinOperator, SerializableOperator& serializedOperator);

    /**
     * @brief Deserializes the SerializableOperator_JoinDetails and all its properties back to a join operatorNodePtr
     * @param sinkDetails The serialized sink operator details.
     * @param operatorId: id of the operator to be deserialized
     * @return LogicalJoinOperator of type LogicalUnaryOperator
     */
    static LogicalJoinOperatorPtr deserializeJoinOperator(const SerializableOperator_JoinDetails& joinDetails,
                                                              OperatorId operatorId);

    /**
     * @brief Serializes an batch join operator and all its properties to a SerializableOperator_JoinDetails object.
     * @param LogicalBatchJoinOperatorPtr The window operator node.
     * @param batchJoinDetails the serialized SerializableOperator_BatchJoinDetails.
     * @param serializedOperator serialized instance of the operator
     */
    static void serializeBatchJoinOperator(const Experimental::LogicalBatchJoinOperator& joinOperator,
                                           SerializableOperator& serializedOperator);

    /**
     * @brief Deserializes the SerializableOperator_BatchJoinDetails and all its properties back to a join operatorNodePtr
     * @param sinkDetails The serialized sink operator details.
     * @param operatorId: id of the operator to be deserialized
     * @return LogicalBatchJoinOperator of type LogicalUnaryOperator
     */
    static Experimental::LogicalBatchJoinOperatorPtr
    deserializeBatchJoinOperator(const SerializableOperator_BatchJoinDetails& joinDetails, OperatorId operatorId);

    /**
     * @brief Serializes an source descriptor and all its properties to a SerializableOperator_SourceDetails object.
     * @param sourceDescriptor The source descriptor.
     * @param sourceDetails the serialized SourceDetails
     * @param isClientOriginated Indicate if the source operator is originated from a client
     */
    static void serializeSourceDescriptor(const SourceDescriptor& sourceDescriptor,
                                          SerializableOperator_SourceDetails& sourceDetails,
                                          bool isClientOriginated = false);

    /**
     * @brief Deserializes the SerializableOperator_SourceDetails and all its properties back to a sink SourceDescriptorPtr.
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
     * @brief Deserializes the SerializableOperator_SinkDetails and all its properties back to a sink SinkDescriptorPtr.
     * @param sinkDetails The serialized sink operator details.
     * @return OperatorPtr
     */
    static SinkDescriptorPtr deserializeSinkDescriptor(const SerializableOperator_SinkDetails& sinkDetails);

    /**
     * @brief Serializes the limit operator
     * @param limit logical operator node
     * @param serializedOperator serialized instance of the operator
     */
    static void serializeLimitOperator(const LogicalLimitOperator& limitLogicalOperator,
                                       SerializableOperator& serializedOperator);

    /**
     * @brief Deserializes a limit operator
     * @param LimitDetails
     * @return LogicalLimitOperator of type LogicalUnaryOperator
     */
    static LogicalUnaryOperatorPtr deserializeLimitOperator(const SerializableOperator_LimitDetails& limitDetails);

    /**
     * @brief Serializes the watermarkAssigner operator
     * @param watermark assigner logical operator node
     * @param serializedOperator serialized instance of the operator
     */
    static void serializeWatermarkAssignerOperator(const WatermarkAssignerLogicalOperator& watermarkAssignerOperator,
                                                   SerializableOperator& serializedOperator);

    /**
     * @brief Deserializes a watermarkAssigner operator
     * @param watermarkStrategyDetails
     * @return WatermarkAssignerLogicalOperator of type LogicalUnaryOperator
     */
    static LogicalUnaryOperatorPtr
    deserializeWatermarkAssignerOperator(const SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails);

    /**
     * @brief Serializes a watermark strategy descriptor
     * @param watermarkStrategyDescriptor The watermark strategy descriptor
     * @param watermarkStrategyDetails The watermark strategy details object
     */
    static void serializeWatermarkStrategyDescriptor(const Windowing::WatermarkStrategyDescriptor& watermarkStrategyDescriptor,
                                                     SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails);

    /**
     * @brief Deserialize to WatermarkStrategyDescriptor
     * @param watermarkStrategyDetails details of serializable watermarkstrategy
     * @return WatermarkStrategyDescriptor
     */
    static Windowing::WatermarkStrategyDescriptorPtr
    deserializeWatermarkStrategyDescriptor(const SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails);

    /**
     * @brief Serializes an input schema
     * @param operatorNode node for which to serialize the input schema
     * @param serializedOperator serialized instance of the operator
     */
    static void serializeInputSchema(const OperatorPtr& operatorNode, SerializableOperator& serializedOperator);

    /**
     * @brief Deserializes an input schema
     * @param serializedOperator serialized operator
     * @param operatorNode LogicalOperator for which to serialize the input schema
     */
    static void deserializeInputSchema(LogicalOperatorPtr operatorNode, SerializableOperator& serializedOperator);

    /**
     * @brief Serializes an inferModel logical operator
     * @param inferModel operator
     * @param serializedOperator serialized instance of the operator
     */
    static void serializeInferModelOperator(const InferModel::LogicalInferModelOperator& inferModel,
                                            SerializableOperator& serializedOperator);

    /**
     * @brief Deserializes an inferModel logical operator
     * @param inferModelDetails
     * @return LogicalUnaryOperator of type InferModel::LogicalInferModelOperator
     */
    static LogicalUnaryOperatorPtr
    deserializeInferModelOperator(const SerializableOperator_InferModelDetails& inferModelDetails);

    /**
     * @brief Serializes a Map or FlatMap Java UDF operator
     * @param mapJavaUdfOperator
     * @param serializedOperator serialized instance of the operator
     * @tparam T The LogicalOperator (either MapJavaUDFLogicalOperator or FlatMapJavaUDFLogicalOperator)
     * @tparam D The GRPC SerializeOperator details message (either SerializableOperator_MapJavaUdfDetails or SerializableOperator_FlatMapJavaUdfDetails)
     */
    template<typename T, typename D>
    static void serializeJavaUDFOperator(const T& mapJavaUDFOperator, SerializableOperator& serializedOperator);

    /**
     * @brief deserializes a Map Java UDF operator
     * @param mapJavaUdfDetails
     * @return MapJavaUdfLogicalOperatorPtr
     */
    static LogicalUnaryOperatorPtr
    deserializeMapJavaUDFOperator(const SerializableOperator_MapJavaUdfDetails& mapJavaUDFDetails);

    /**
     * @brief deserializes a FlatMap Java UDF operator
     * @param flatMapJavaUDFDetails
     * @return MapJavaUdfLogicalOperatorPtr
     */
    static LogicalUnaryOperatorPtr
    deserializeFlatMapJavaUDFOperator(const SerializableOperator_FlatMapJavaUdfDetails& flatMapJavaUDFDetails);

    /**
     * @brief deserialize open cl operator
     * @param openCLLogicalOperator
     * @param serializedOperator
     */
    static void serializeOpenCLOperator(const NES::LogicalOpenCLOperator& openCLLogicalOperator,
                                        SerializableOperator& serializedOperator);

    /**
     * @brief serialize open cl operator
     * @param openCLDetails
     * @return LogicalOpenCLOperatorPtr
     */
    static LogicalUnaryOperatorPtr deserializeOpenCLOperator(const SerializableOperator_OpenCLOperatorDetails& openCLDetails);
};
}// namespace NES

#endif // NES_OPERATORS_INCLUDE_OPERATORS_SERIALIZATION_OPERATORSERIALIZATIONUTIL_HPP_
