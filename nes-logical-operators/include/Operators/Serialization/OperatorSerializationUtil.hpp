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
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/MapLogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowOperator.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{


/// The OperatorSerializationUtil offers functionality to serialize and deserialize logical operator trees to a corresponding protobuffer object.
class OperatorSerializationUtil
{
public:
    /// Serializes an operator node and all its children to a SerializableOperator object.
    static SerializableOperator serializeOperator(LogicalOperator operatorNode);

    /// Deserializes the input SerializableOperator only
    /// Note: This method will not deserialize its children
    static LogicalOperator deserializeOperator(SerializableOperator serializedOperator);

    static LogicalOperator deserializeLogicalOperator(const SerializableOperator_LogicalOperator& serializedOperator);

    static void serializeSourceOperator(const SourceDescriptorLogicalOperator& sourceOperator, SerializableOperator& serializedOperator);
    static void serializeSinkOperator(const SinkLogicalOperator& sinkOperator, SerializableOperator& serializedOperator);
    static void serializeSourceDescriptor(
        const Sources::SourceDescriptor& sourceDescriptor, SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails);
    static void serializeSinkDescriptor(const Schema& schema, const Sinks::SinkDescriptor& sinkDescriptor, SerializableOperator_SinkLogicalOperator& sinkDetails);

    static Sources::SourceDescriptor
    static LogicalOperator deserializeSourceOperator(const SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails);
    static LogicalOperator deserializeSinkOperator(const SerializableOperator_SinkLogicalOperator& sinkDetails);
    deserializeSourceDescriptor(const SerializableOperator_SourceDescriptorLogicalOperator_SourceDescriptor& sourceDescriptor);
    static Sinks::SinkDescriptor
    deserializeSinkDescriptor(const SerializableOperator_SinkLogicalOperator_SerializableSinkDescriptor& serializableSinkDescriptor);

    static std::unique_ptr<WindowAggregationLogicalFunction>
    deserializeWindowAggregationFunction(const SerializableOperator_SinkLogicalOperator_SerializableSinkDescriptor& serializableWindowAggregationFunction);

};
}
