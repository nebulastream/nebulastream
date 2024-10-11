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

#pragma once

#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinOperator.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

class SerializableOperator;
class SerializableOperator_SinkDetails;
class SerializableOperator_WindowDetails;
class SerializableOperator_JoinDetails;
class SerializableOperator_BatchJoinDetails;
class SerializableOperator_WatermarkStrategyDetails;
class SerializableOperator_LimitDetails;
class SerializableOperator_MapDetails;
class SerializableOperator_InferModelDetails;
class SerializableOperator_CEPIterationDetails;
class SerializableOperator_ProjectionDetails;
class SerializableOperator_FilterDetails;
class SerializableOperator_UnionDetails;
class SerializableOperator_BroadcastDetails;

/// The OperatorSerializationUtil offers functionality to serialize and deserialize logical operator trees to a corresponding protobuffer object.
class OperatorSerializationUtil
{
public:
    /// Serializes an operator node and all its children to a SerializableOperator object.
    static SerializableOperator serializeOperator(const OperatorPtr& operatorNode);


    /// Deserializes the input SerializableOperator only
    /// Note: This method will not deserialize its children
    static OperatorPtr deserializeOperator(SerializableOperator serializedOperator);

    static void serializeSourceOperator(SourceDescriptorLogicalOperator& sourceOperator, SerializableOperator& serializedOperator);

    static LogicalUnaryOperatorPtr deserializeSourceOperator(const SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails);

    static void serializeFilterOperator(const LogicalFilterOperator& filterOperator, SerializableOperator& serializedOperator);

    static LogicalUnaryOperatorPtr deserializeFilterOperator(const SerializableOperator_FilterDetails& filterDetails);

    static void serializeProjectionOperator(const LogicalProjectionOperator& projectionOperator, SerializableOperator& serializedOperator);

    static LogicalUnaryOperatorPtr deserializeProjectionOperator(const SerializableOperator_ProjectionDetails& projectionDetails);

    static void serializeSinkOperator(const SinkLogicalOperator& sinkOperator, SerializableOperator& serializedOperator);

    static LogicalUnaryOperatorPtr deserializeSinkOperator(const SerializableOperator_SinkDetails& sinkDetails);

    static void serializeMapOperator(const LogicalMapOperator& mapOperator, SerializableOperator& serializedOperator);

    static LogicalUnaryOperatorPtr deserializeMapOperator(const SerializableOperator_MapDetails& mapDetails);

    static void serializeWindowOperator(const WindowOperator& windowOperator, SerializableOperator& serializedOperator);

    static LogicalUnaryOperatorPtr
    deserializeWindowOperator(const SerializableOperator_WindowDetails& windowDetails, OperatorId operatorId);

    static void serializeJoinOperator(const LogicalJoinOperator& joinOperator, SerializableOperator& serializedOperator);

    static LogicalJoinOperatorPtr deserializeJoinOperator(const SerializableOperator_JoinDetails& joinDetails, OperatorId operatorId);

    static void
    serializeBatchJoinOperator(const Experimental::LogicalBatchJoinOperator& joinOperator, SerializableOperator& serializedOperator);

    static Experimental::LogicalBatchJoinOperatorPtr
    deserializeBatchJoinOperator(const SerializableOperator_BatchJoinDetails& joinDetails, OperatorId operatorId);

    static void serializeSourceDescriptor(
        const Sources::SourceDescriptor& sourceDescriptor, SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails);

    /**
     * @brief Deserializes the SerializableOperator_SourceDetails and all its properties back to a sink SourceDescriptorPtr.
     * @param sourceDetails The serialized source operator details.
     * @return SourceDescriptorPtr
     */
    static std::unique_ptr<Sources::SourceDescriptor>
    deserializeSourceDescriptor(const SerializableOperator_SourceDescriptorLogicalOperator_SourceDescriptor& sourceDescriptor);

    /**
     * @brief Serializes an sink descriptor and all its properties to a SerializableOperator_SinkDetails object.
     * @param sinkDescriptor The sink descriptor.
     * @param sinkDetails The sink details object.
     * @param numberOfOrigins the number of origins
     */
    static void
    serializeSinkDescriptor(const SinkDescriptor& sinkDescriptor, SerializableOperator_SinkDetails& sinkDetails, uint64_t numberOfOrigins);

    static SinkDescriptorPtr deserializeSinkDescriptor(const SerializableOperator_SinkDetails& sinkDetails);

    static void serializeLimitOperator(const LogicalLimitOperator& limitLogicalOperator, SerializableOperator& serializedOperator);


    static LogicalUnaryOperatorPtr deserializeLimitOperator(const SerializableOperator_LimitDetails& limitDetails);

    static void serializeWatermarkAssignerOperator(
        const WatermarkAssignerLogicalOperator& watermarkAssignerOperator, SerializableOperator& serializedOperator);

    static LogicalUnaryOperatorPtr
    deserializeWatermarkAssignerOperator(const SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails);

    static void serializeWatermarkStrategyDescriptor(
        const Windowing::WatermarkStrategyDescriptor& watermarkStrategyDescriptor,
        SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails);

    static Windowing::WatermarkStrategyDescriptorPtr
    deserializeWatermarkStrategyDescriptor(const SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails);

    static void serializeInputSchema(const OperatorPtr& operatorNode, SerializableOperator& serializedOperator);

    static void deserializeInputSchema(LogicalOperatorPtr operatorNode, const SerializableOperator& serializedOperator);

    static void
    serializeInferModelOperator(const InferModel::LogicalInferModelOperator& inferModel, SerializableOperator& serializedOperator);

    static LogicalUnaryOperatorPtr deserializeInferModelOperator(const SerializableOperator_InferModelDetails& inferModelDetails);
};
} /// namespace NES
