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
#include <Operators/LogicalOperators/LogicalDelayBufferOperator.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalSortBufferOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

class SerializableOperator;
class SerializableOperator_WindowDetails;
class SerializableOperator_JoinDetails;
class SerializableOperator_BatchJoinDetails;
class SerializableOperator_WatermarkStrategyDetails;
class SerializableOperator_LimitDetails;
class SerializableOperator_MapDetails;
class SerializableOperator_InferModelDetails;
class SerializableOperator_CEPIterationDetails;
class SerializableOperator_ProjectionDetails;
class SerializableOperator_SelectionDetails;
class SerializableOperator_UnionDetails;
class SerializableOperator_BroadcastDetails;

/// The OperatorSerializationUtil offers functionality to serialize and deserialize logical operator trees to a corresponding protobuffer object.
class OperatorSerializationUtil
{
public:
    /// Serializes an operator node and all its children to a SerializableOperator object.
    static SerializableOperator serializeOperator(const std::shared_ptr<LogicalOperator>& operatorNode);

    /// Deserializes the input SerializableOperator only
    /// Note: This method will not deserialize its children
    static std::shared_ptr<LogicalOperator> deserializeOperator(SerializableOperator serializedOperator);

    static void serializeSourceOperator(SourceDescriptorLogicalOperator& sourceOperator, SerializableOperator& serializedOperator);

    static LogicalUnaryOperatorPtr deserializeSourceOperator(const SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails);

    static void serializeSinkOperator(const SinkLogicalOperator& sinkOperator, SerializableOperator& serializedOperator);

    static LogicalUnaryOperatorPtr deserializeSinkOperator(const SerializableOperator_SinkLogicalOperator& sinkDetails);

    static void serializeSelectionOperator(const LogicalSelectionOperator& selectionOperator, SerializableOperator& serializedOperator);

    static void serializeSortBufferOperator(const LogicalSortBufferOperator& sortBufferOperator, SerializableOperator& serializedOperator);

    static LogicalUnaryOperatorPtr deserializeSortBufferOperator(const SerializableOperator_SortBufferDetails& sortBufferDetails);

    static void serializeProjectionOperator(const LogicalProjectionOperator& projectionOperator, SerializableOperator& serializedOperator);

    static void serializeMapOperator(const LogicalMapOperator& mapOperator, SerializableOperator& serializedOperator);

    static void serializeWindowOperator(const WindowOperator& windowOperator, SerializableOperator& serializedOperator);

    static void serializeJoinOperator(const LogicalJoinOperator& joinOperator, SerializableOperator& serializedOperator);

    static void
    serializeBatchJoinOperator(const Experimental::LogicalBatchJoinOperator& joinOperator, SerializableOperator& serializedOperator);

    static void serializeSourceDescriptor(
        const Sources::SourceDescriptor& sourceDescriptor, SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails);

    static std::unique_ptr<Sources::SourceDescriptor>
    deserializeSourceDescriptor(const SerializableOperator_SourceDescriptorLogicalOperator_SourceDescriptor& sourceDescriptor);

    static void serializeSinkDescriptor(
        std::shared_ptr<Schema> schema, const Sinks::SinkDescriptor& sinkDescriptor, SerializableOperator_SinkLogicalOperator& sinkDetails);

    static std::unique_ptr<Sinks::SinkDescriptor>
    deserializeSinkDescriptor(const SerializableOperator_SinkLogicalOperator_SerializableSinkDescriptor& serializableSinkDescriptor);

    static void serializeLimitOperator(const LogicalLimitOperator& limitLogicalOperator, SerializableOperator& serializedOperator);

    static void serializeWatermarkAssignerOperator(
        const WatermarkAssignerLogicalOperator& watermarkAssignerOperator, SerializableOperator& serializedOperator);

    static void serializeWatermarkStrategyDescriptor(
        const Windowing::WatermarkStrategyDescriptor& watermarkStrategyDescriptor,
        SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails);

    static Windowing::WatermarkStrategyDescriptorPtr
    deserializeWatermarkStrategyDescriptor(const SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails);

    static void serializeInputSchema(const OperatorPtr& operatorNode, SerializableOperator& serializedOperator);

    static void deserializeInputSchema(LogicalOperatorPtr operatorNode, const SerializableOperator& serializedOperator);

    static void
    serializeInferModelOperator(const InferModel::LogicalInferModelOperator& inferModel, SerializableOperator& serializedOperator);
};
}
