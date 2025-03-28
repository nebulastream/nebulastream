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
#include <Operators/LogicalOperators/InferModelLogicalOperator.hpp>
#include <Operators/LogicalOperators/MapLogicalOperator.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperator.hpp>
#include <Operators/LogicalOperators/SelectionLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{


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
    static void serializeSinkOperator(const SinkLogicalOperator& sinkOperator, SerializableOperator& serializedOperator);
    static void serializeSourceDescriptor(
        const Sources::SourceDescriptor& sourceDescriptor, SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails);
    static void serializeSinkDescriptor(
        std::shared_ptr<Schema> schema, const Sinks::SinkDescriptor& sinkDescriptor, SerializableOperator_SinkLogicalOperator& sinkDetails);

    static std::shared_ptr<UnaryLogicalOperator>
    deserializeSourceOperator(const SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails);
    static std::shared_ptr<UnaryLogicalOperator> deserializeSinkOperator(const SerializableOperator_SinkLogicalOperator& sinkDetails);
    static std::unique_ptr<Sources::SourceDescriptor>
    deserializeSourceDescriptor(const SerializableOperator_SourceDescriptorLogicalOperator_SourceDescriptor& sourceDescriptor);
    static std::unique_ptr<Sinks::SinkDescriptor>
    deserializeSinkDescriptor(const SerializableOperator_SinkLogicalOperator_SerializableSinkDescriptor& serializableSinkDescriptor);

    static void serializeSelectionOperator(const SelectionLogicalOperator& selectionOperator, SerializableOperator& serializedOperator);
    static void serializeProjectionOperator(const ProjectionLogicalOperator& projectionOperator, SerializableOperator& serializedOperator);
    static void serializeMapOperator(const MapLogicalOperator& mapOperator, SerializableOperator& serializedOperator);
    static void serializeWindowOperator(const WindowOperator& windowOperator, SerializableOperator& serializedOperator);
    static void serializeJoinOperator(const JoinLogicalOperator& joinOperator, SerializableOperator& serializedOperator);

    static void serializeWatermarkStrategyDescriptor(
        const Windowing::WatermarkStrategyDescriptor& watermarkStrategyDescriptor,
        SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails);
    static std::shared_ptr<Windowing::WatermarkStrategyDescriptor>
    deserializeWatermarkStrategyDescriptor(const SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails);

    static void serializeInputSchema(const std::shared_ptr<Operator>& operatorNode, SerializableOperator& serializedOperator);
    static void deserializeInputSchema(std::shared_ptr<LogicalOperator> operatorNode, const SerializableOperator& serializedOperator);

    static void serializeWatermarkAssignerOperator(
        const WatermarkAssignerLogicalOperator& watermarkAssignerOperator, SerializableOperator& serializedOperator);

    static void
    serializeInferModelOperator(const InferModel::InferModelLogicalOperator& inferModel, SerializableOperator& serializedOperator);
};
}
