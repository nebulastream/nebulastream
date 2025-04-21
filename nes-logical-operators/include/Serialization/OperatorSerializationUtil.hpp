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
#include <Operators/LogicalOperator.hpp>
#include <Operators/MapLogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{


/// The OperatorSerializationUtil offers functionality to serialize and deserialize logical operator trees to a corresponding protobuffer object.
class OperatorSerializationUtil
{
public:
    /// Deserializes the input SerializableOperator only
    /// Note: This method will not deserialize its children
    static LogicalOperator deserializeOperator(SerializableOperator serializedOperator);
    static LogicalOperator deserializeLogicalOperator(const SerializableOperator_LogicalOperator& serializedOperator);
    static LogicalOperator deserializeSourceOperator(const SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails);
    static LogicalOperator deserializeSinkOperator(const SerializableOperator_SinkLogicalOperator& sinkDetails);
    static std::unique_ptr<Sources::SourceDescriptor>
    deserializeSourceDescriptor(const SerializableOperator_SourceDescriptorLogicalOperator_SourceDescriptor& sourceDescriptor);
    static std::unique_ptr<Sinks::SinkDescriptor>
    deserializeSinkDescriptor(const SerializableOperator_SinkLogicalOperator_SerializableSinkDescriptor& serializableSinkDescriptor);
    static std::shared_ptr<WindowAggregationLogicalFunction> deserializeWindowAggregationFunction(
        const SerializableOperator_SinkLogicalOperator_SerializableSinkDescriptor& serializableWindowAggregationFunction);
};
}
